import re
import sys
import time
import decimal

ctx = decimal.Context()
ctx.prec = 20

def float_to_str(f):
    """
    Convert the given float to a string,
    without resorting to scientific notation
    """
    d1 = ctx.create_decimal(repr(f))
    return format(d1, 'f')

def myToStr(a):
    if type(a) is float:
        return float_to_str(a)
    return str(a)

master_template="""
:sql_request_bed
CREATE TABLE :table_room_score AS 
(
    :sql_room_score
);

CREATE TABLE :table_topsis AS 
(
    SELECT
        h.id,
        :s_topsis_room * room_score as room_score,
        :s_topsis_distance * (:k_dist_ac / (:k_dist_ac + distance)) as dist_score,
        :s_topsis_star * (:k_star_ac / (:k_star_ac + abs(:request_star - COALESCE(h.star_rating, 0)))) as star_score,
        :s_topsis_services * (service_count / :var_hotel_service_count) as service_score
    FROM (
        SELECT 
            hotel_id,
            sum(score) as room_score
        FROM 
        (
            SELECT hotel_id, (owa.weight * room_score.score) as score
            FROM :table_room_score room_score
            LEFT JOIN owa_weight owa
            ON room_score.owa_id = owa.id
        ) rom
        GROUP BY hotel_id
    ) room_score
    LEFT JOIN 
    (
        SELECT 
            *,
            2 * :k_earth_r * asin(sqrt(sin((:request_latitude - h.latitude)/2)*sin((:request_latitude - h.latitude)/2) + cos(:request_latitude) * cos(h.latitude) * sin((:request_longitude - h.longitude)/2) * sin((:request_longitude - h.longitude)/2))) as distance
        FROM
        hotels h
    ) h
    ON h.id = room_score.hotel_id
    LEFT JOIN
    (
        SELECT h.id, COUNT(hfr.feature_id) as service_count
        from hotels h, hotel_feature_relations hfr
        WHERE hfr.hotel_id  = h.id AND hfr.feature_id in (:request_hotel_feature)
        GROUP BY h.id
    ) sercount 
    ON h.id = sercount.id
);

SELECT rnk.hotel_id as hotelId, name, address, star_rating as star, roomIds, COALESCE(service_names, ''), image_url as hotelServices
FROM
(
    SELECT hotel_id, dw/(dw+db) as topsis_score 
    from (
        SELECT 
            id as hotel_id, 
            sqrt((room_score-t1)*(room_score - t1)+(dist_score-t2)*(dist_score-t2)+(star_score-t3)*(star_score-t3)+(service_score - t4)*(service_score-t4)) as dw,  
            sqrt((room_score-k1)*(room_score - k1)+(dist_score-k2)*(dist_score-k2)+(star_score-k3)*(star_score-k3)+(service_score - k4)*(service_score-k4)) as db
        from
        :table_topsis c,
        (
            SELECT 
                max(room_score) as t1, 
                max(dist_score) as t2, 
                max(star_score) as t3, 
                max(service_score) as t4, 
                min(room_score) as k1, 
                min(dist_score) as k2,
                min(star_score) as k3,
                min(service_score) as k4 
            from :table_topsis
        ) d
    ) k
    ORDER BY topsis_score DESC 
    LIMIT :k_offset, :k_limit 
) rnk
LEFT JOIN 
(
    SELECT hotel_id, GROUP_CONCAT(room_id ORDER BY score) as roomIds
    FROM :table_room_score
    GROUP BY hotel_id
) roomsss
ON rnk.hotel_id = roomsss.hotel_id
LEFT join
	hotels h
ON h.id = rnk.hotel_id
LEFT join
(
	SELECT hfr.hotel_id, COALESCE(GROUP_CONCAT(hf.name), '') as service_names
	FROM hotel_feature_relations hfr, hotel_features hf
    WHERE hfr.feature_id in (:request_hotel_feature) AND hf.id = hfr.feature_id 
	GROUP BY hfr.hotel_id 
) seee
ON rnk.hotel_id = seee.hotel_id;
:sql_request_bed_delete """

room_score_template="""SELECT 
    *,  
    row_number() over (partition by hotel_id order by score) as owa_id
FROM (
SELECT 
	r.id as room_id, r.hotel_id,
	(
	                    case
	                    when (:k_price_low * r.cheapest_price + (1-:k_price_low) * r.before_discount_price) < :request_price_low then (0.25 * :request_price_low + 0.25 * :request_price_high)/(:request_price_low -  (:k_price_low * r.cheapest_price + (1-:k_price_low) * r.before_discount_price) + (0.25 * :request_price_low + 0.25 * :request_price_high))
	                    when (:k_price_low * r.cheapest_price + (1-:k_price_low) * r.before_discount_price) > :request_price_high then (0.25 * :request_price_low + 0.25 * :request_price_high)/( (:k_price_low * r.cheapest_price + (1-:k_price_low) * r.before_discount_price) - :request_price_high + (0.25 * :request_price_low + 0.25 * :request_price_high))
	                    else 1
	                    end
	) * :s_pr +
	COALESCE(r.`view` = :request_view, 0) * :s_vi +
	COALESCE(facility, 0) * :s_fa +
	COALESCE(feature, 0) * :s_fe +
    bed_score.bed_score * :s_be
	as score
from 
	rooms r
	left join
	(
		SELECT room_id as id, count(1) / :var_facility_count as facility
		FROM room_facility_relations rfr 
		WHERE facility_id in (:request_room_facility)
		group by room_id
	) tmp_room_facility
	on r.id = tmp_room_facility.id
	left join
	(
		SELECT room_id as id, count(1) / :var_service_count as feature
		FROM room_feature_relations rfr 
		WHERE feature_id in (:request_room_service)
		group by room_id
	) tmp_room_feature
	on r.id = tmp_room_feature.id
    left join
    (
        SELECT id, avg(score) as bed_score from (
            SELECT id, 
            case 
                when d > 0 then least(d*:s_bed_alpha + :s_bed_base,1)
                when d < 0 then greatest(d*:s_bed_beta + :s_bed_base,0)
                else 0
            end score
            FROM (
                SELECT r.id, COALESCE(rbr.count, 0) - t.count as d
                FROM
                    :table_bed_request t
                    cross join
                    rooms r
                    left join 
                    room_bed_relations rbr 
                    on r.id = rbr.room_id and t.bed_id = rbr.bed_id
            ) bed_dist
        ) as bed_score
        group by id
    ) bed_score
    on r.id = bed_score.id
) scored"""
 
class SQLCompiler:
    def __init__(self, sql):
        self.sql = sql
        self.st = []
        self.ed = []
        self.words = []
        for match in re.finditer("(:[^ \\),\\+,\\n;]+)[ \\),\\+,\\n;]", self.sql):
            self.st.append(match.start(1))
            self.ed.append(match.end(1))
            self.words.append(match.group(1)[1:])

    def compile(self, p) -> str:
        out = ""
        last = 0
        for i in range(len(self.st)):
            out += self.sql[last:self.st[i]]
            out += myToStr(p.__dict__[self.words[i]])
            last = self.ed[i]
        out += self.sql[last:]
        return out
 
room_score_compiler = SQLCompiler(room_score_template)
master_compiler = SQLCompiler(master_template)

class Parameter:
    def __init__(self):
        #super parameter when calculate room score
        self.s_fa = 1        #facility
        self.s_fe = 1        #feature
        self.s_vi = 1        #view
        self.s_pr = 1        #price
        self.s_be = 1        #bed
        self.s_bed_alpha = 1        #bed
        self.s_bed_beta = 1        #bed
        self.s_bed_base = 1        #bed

        #super parameter when normalize hotel table for topsis
        self.s_topsis_room = 1
        self.s_topsis_services= 1
        self.s_topsis_distance= 1
        self.s_topsis_star= 1

        #request input
        self.request_view = 'NULL'
        self.request_double_bed = 'NULL'
        self.request_single_bed = 'NULL'
        self.request_sofa_bed = 'NULL'
        self.request_king_bed = 'NULL'
        self.request_queen_bed = 'NULL'
        self.request_super_king_bed = 'NULL'
        self.request_semi_double_bed = 'NULL'
        self.request_bunk_bed = 'NULL'
        self.request_japanese = 'NULL'
        self.request_price_low = 'NULL'
        self.request_price_high = 'NULL'
        self.request_room_facility = 'NULL'
        self.request_room_service = 'NULL'
        self.request_longitude = 'NULL'
        self.request_latitude = 'NULL'
        self.request_hotel_feature = 'NULL'
        self.request_star = 'NULL'

        #constant
        self.k_price_low = 0.3       
        self.k_epsilon = 0.000000000001
        self.k_offset = 0
        self.k_limit = 10
        self.k_dist_ac = 2000
        self.k_star_ac = 1
        self.k_earth_r = 6371000

    def createTables(self):
        sql = """CREATE TABLE :table_bed_request (bed_id bigint, count int);
INSERT INTO :table_bed_request
VALUES 
"""
        requests = ['request_super_king_bed', 'request_sofa_bed', 'request_king_bed', 'request_single_bed', 'request_double_bed', 'request_queen_bed', 'request_bunk_bed', 'request_japanese', 'request_semi_double_bed', 'request_japanese']
        j = 0
        for i in requests:
            if self.__dict__[i] != 'NULL' and self.__dict__[i].isnumeric():
                sql += "(" + str(j) + ","+ self.__dict__[i] +"),"
            j = j+1
        sql = sql[:-1] + ';'
        self.sql_request_bed = SQLCompiler(sql).compile(self)
        print("request_bed: \n"+self.sql_request_bed, file=sys.stderr)
        self.sql_request_bed_delete = SQLCompiler("DROP TABLE :table_bed_request ;\nDROP TABLE :table_topsis ;\nDROP TABLE :table_room_score ;").compile(self)
        print("request_bed_delete: \n"+self.sql_request_bed_delete, file=sys.stderr)

    def processSpecial(self):
        transId = int(time.time_ns())
        self.table_bed_request = 'bed_request_'+str(transId)
        self.table_topsis = 'topsis_'+str(transId)
        self.table_room_score = 'room_score_'+str(transId)
        self.createTables()

        if self.request_view[0:1] != "'":
            self.request_view = "'" + self.request_view
        if self.request_view[-1:] != "'":
            self.request_view = self.request_view + "'" 
        self.var_facility_count = len(self.request_room_facility.split(","))
        self.var_hotel_service_count = len(self.request_hotel_feature.split(","))
        self.var_service_count = len(self.request_room_service.split(",")) 
        sql_room_score_teplate = room_score_compiler.compile(self)
        self.sql_room_score=SQLCompiler(sql_room_score_teplate).compile(self)

    def countBedType(self):
        ret = 0
        requests = ['request_double_bed', 'request_single_bed', 'request_sofa_bed', 'request_king_bed', 'request_queen_bed', 'request_super_king_bed', 'request_semi_double_bed', 'request_bunk_bed', 'request_japanese']
        for i in range(len(requests)):
            if self.__dict__[requests[i]] != 'NULL' and self.__dict__[requests[i]].isnumeric():
                ret = ret + 1
        return ret
