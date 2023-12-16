# database_utils.py
import mysql.connector

def connect_to_mysql(host, port, user, password, database):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            auth_plugin='mysql_native_password'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def execute_query(connection, query):
    try:
        print(connection)
        cursor = connection.cursor()

        cursor.execute(query)

        # Lấy kết quả
        result = cursor.fetchall()

        return result
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        # Không đóng cursor ở đây
        pass

import re
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

class Parameter:
    def __init__(self):
        #super parameter when calculate room score
        self.s_fa = 1        #facility
        self.s_fe = 1        #feature
        self.s_vi = 1        #view
        self.s_pr = 1        #price
        self.s_bed_si = 1    #single
        self.s_bed_do = 1    #double
        self.s_bed_so = 1    #sofa
        self.s_bed_ki = 1    #king
        self.s_bed_qu = 1    #queen
        self.s_bed_su = 1    #superking
        self.s_bed_se = 1    #semi
        self.s_bed_bu = 1    #bunk
        self.s_jap = 1       #japanese futon

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
        self.request_longtitude = 'NULL'
        self.request_latitude = 'NULL'
        self.request_hotel_feature = 'NULL'
        self.request_star = 'NULL'

        #constant
        self.k_price_low = 0.3       
        self.k_epsilon = 0.000000000001
        self.k_offset = 0
        self.k_limit = 10

    def processSpecial(self):
        requests = ['request_double_bed', 'request_single_bed', 'request_sofa_bed', 'request_king_bed', 'request_queen_bed', 'request_super_king_bed', 'request_semi_double_bed', 'request_bunk_bed', 'request_japanese']
        supers = ['s_bed_do', 's_bed_si', 's_bed_so', 's_bed_ki', 's_bed_qu', 's_bed_su', 's_bed_se', 's_bed_bu', 's_jap']
        for i in range(len(requests)):
            if self.__dict__[requests[i]] != 'NULL' and self.__dict__[requests[i]].isnumeric():
                pass
            else:
                self.__dict__[supers[i]] = 0
        if self.request_view[0:1] != "'":
            self.request_view = "'" + self.request_view
        if self.request_view[-1:] != "'":
            self.request_view = self.request_view + "'" 
 

class SQLCompiler:
    _BASE_SQL = """CREATE TABLE topsis_data AS
(
SELECT hotel_id, room_score+:k_epsilon as room_score, distance+:k_epsilon as distance, service_count+:k_epsilon as service_count, abs(star_rating - :request_star)+:k_epsilon as star_rating
FROM
(
    SELECT hotel_id, sum(score * owa_weight.v) as room_score
    FROM 
    (
        SELECT 
            aa.id as id, 
            hotel_id, 
            COALESCE(facility, 0)* :s_fa + 
            COALESCE(feature, 0) * :s_fe +
            view_match * :s_vi +
            atan(price * :s_pr)/PI()  +
            single_bed * :s_bed_si +
            double_bed * :s_bed_do +
            sofa_bed * :s_bed_so +
            king_bed * :s_bed_ki +
            queen_bed * :s_bed_qu +
            super_king_bed * :s_bed_su +
            semi_double_bed * :s_bed_se +
            bunk_bed * :s_bed_bu +
            japanese_futon * :s_jap as score,
            ROW_NUMBER() over (PARTITION by hotel_id ORDER BY score) as groupRow
        FROM
        (
            SELECT 
                r.id, 
                r.hotel_id, 
                COALESCE(r.`view` = :request_view, 0) as view_match,
                COALESCE(abs(r.double_bed  - :request_double_bed),0) as double_bed,
                COALESCE(abs(r.single_bed  - :request_single_bed),0) as single_bed,
                COALESCE(abs(r.sofa_bed  - :request_sofa_bed),0) as sofa_bed,
                COALESCE(abs(r.king_bed  - :request_king_bed),0) as king_bed,
                COALESCE(abs(r.queen_bed  - :request_queen_bed),0) as queen_bed,
                COALESCE(abs(r.super_king_bed  - :request_super_king_bed),0) as super_king_bed,
                COALESCE(abs(r.semi_double_bed  - :request_semi_double_bed),0) as semi_double_bed,
                COALESCE(abs(r.bunk_bed  - :request_bunk_bed),0) as bunk_bed,
                COALESCE(abs(r.japanese_futon  - :request_japanese),0) as japanese_futon,
              (
                    case
                    when aprice < :request_price_low then :request_price_low - aprice
                    when aprice > :request_price_high then (aprice - :request_price_high)
                    else 0
                    end
                ) as price 
            FROM 
            (
                SELECT *, :k_price_low * r.cheapest_price + (1-:k_price_low) * r.before_discount_price as aprice FROM rooms r
            ) r
        ) aa 
        left join 
        (
            SELECT r.id, COUNT(*) as facility
            FROM room_facility_relations rfr , rooms r 
            WHERE 
            r.id  = rfr.room_id AND 
            rfr.facility_id in (:request_room_facility) 
            GROUP BY r.id
        ) bb
        on aa.id = bb.id
        LEFT JOIN
        (
            SELECT r.id, COUNT(*) as feature
            FROM room_feature_relations rfr, rooms r
            WHERE 
            r.id  = rfr.room_id AND 
            rfr.feature_id  in (:request_room_service) 
            GROUP BY r.id
        ) cc
        on aa.id = cc.id
        order by hotel_id, score
    ) dd
    LEFT JOIN 
        owa_weight
    ON owa_weight.id = dd.groupRow
    GROUP BY hotel_id
) ee 
LEFT JOIN
(
    SELECT hh.id, 6371000*2*ATAN2(SQRT(a)*SQRT(1-a)) as distance, COALESCE(cnt, 0) as service_count, star_rating FROM (
        SELECT id, sin(delta_lat/2)*sin(delta_lat/2) + cos(lat1)*cos(lat2) * sin(delta_lon/2) * sin(delta_lon/2) as a, star_rating from (
            SELECT id, lat1, lat2, lat1 - lat2 as delta_lat, lot1 - lot2 as delta_lon, star_rating from (
                SELECT id, latitude/360 as lat1, longitude/360 as lot1, :request_longtitude /360 as lot2, :request_latitude /360 as lat2, star_rating, address, name FROM hotels h
            ) k
        ) l
    ) hh
    LEFT JOIN
    (
        SELECT h.id, COUNT(hfr.feature_id) as cnt
        from hotels h, hotel_feature_relations hfr
        WHERE hfr.hotel_id  = h.id AND hfr.feature_id in (:request_hotel_feature)
        GROUP BY h.id
    ) i
    on hh.id = i.id
) ff
ON ee.hotel_id = ff.id
);

create table topsis_calculated as (
select t.hotel_id, :s_topsis_room * t.room_score/t1 as room_score, :s_topsis_star * t.star_rating/t2 as star_rating, :s_topsis_distance * t.distance/t3 as distance, :s_topsis_services * t.service_count/t4 as service_count from 
topsis_data t,
(
	SELECT 
		sqrt(sum(room_score*room_score)) as t1,
		sqrt(sum(star_rating*star_rating)) as t2,
		sqrt(sum(distance*distance)) as t3,
		sqrt(sum(service_count*service_count)) as t4
	from topsis_data t
) p
);

SELECT hotel_id, dw/(dw+db) as topsis_score 
from (
SELECT 
	hotel_id, 
	sqrt((room_score-t1)*(room_score - t1)+(distance-t2)*(distance-t2)+(star_rating-t3)*(star_rating-t3)+(service_count - t4)*(service_count-t4)) as dw,  
	sqrt((room_score-k1)*(room_score - k1)+(distance-k2)*(distance-k2)+(star_rating-k3)*(star_rating-k3)+(service_count - k4)*(service_count-k4)) as db
from
topsis_calculated c,
(
	SELECT 
		max(room_score) as t1, 
		min(distance) as t2, 
		min(star_rating) as t3, 
		max(service_count) as t4, 
		min(room_score) as k1, 
		max(distance) as k2,
		max(star_rating) as k3,
		min(service_count) as k4 
	from topsis_calculated	
) d
) k
ORDER BY topsis_score DESC 
LIMIT :k_offset, :k_limit 
"""
    def __init__(self):
        self.st = []
        self.ed = []
        self.words = []
        for match in re.finditer("(:[^ \\),]+)[ \\),]", self._BASE_SQL):
            self.st.append(match.start(1))
            self.ed.append(match.end(1))
            self.words.append(match.group(1)[1:])

    def compile(self, p : Parameter) -> str:
        out = ""
        last = 0
        for i in range(len(self.st)):
            out += self._BASE_SQL[last:self.st[i]]
            out += myToStr(p.__dict__[self.words[i]])
            last = self.ed[i]
        out += self._BASE_SQL[last:]
        return out



