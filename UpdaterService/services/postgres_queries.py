
def query_get_cities():
    query = """
        SELECT  "City", "CityId"
        FROM "city_attributes"
    """
    return query


def query_get_cityid_by_city(table, data):
    query = """
        SELECT "City", "Country", "CityId"
        FROM "city_attributes"
        WHERE City='{DATA}'
    """.format(
        TABLE=table,
        DATA=data
    )
    return query


def query_insert_to_city_attributes(data):
    query = """
        INSERT INTO "city_attributes"("City", "Country", "Latitude", "Longitude")
        SELECT a."City", a."Country", a."Latitude", a."Longitude"
        FROM (VALUES {VALUES}) AS a("City", "Country", "Latitude", "Longitude")
        LEFT JOIN "city_attributes" b
            ON a."City"=b."City" AND a."Country"=b."Country"
            WHERE b."City" IS NULL AND b."Country" IS NULL
        ON CONFLICT ("City", "Country") DO NOTHING
        RETURNING *;
    """.format(
        VALUES=data
    )
    return query


def query_update_to_city_attributes(data):
    query = """
        UPDATE "city_attributes" AS t
        SET "Latitude"=c."Latitude", "Longitude"=c."Longitude" 
        FROM (
            SELECT a."City", a."Country", a."Latitude", a."Longitude"
            FROM (VALUES {VALUES}) AS a("City", "Country", "Latitude", "Longitude")
            INNER JOIN "city_attributes" b
            ON (a."City"=b."City" AND a."Country"=b."Country")
            ) AS c("City", "Country", "Latitude", "Longitude")
        WHERE c."City"=t."City" AND c."Country"=t."Country"
        ;
        INSERT INTO "city_attributes"("City", "Country", "Latitude", "Longitude")
        SELECT a."City", a."Country", a."Latitude", a."Longitude"
        FROM ( VALUES {VALUES} ) AS a("City", "Country", "Latitude", "Longitude")
        LEFT JOIN "city_attributes" b
            ON a."City"=b."City" AND a."Country"=b."Country"
            WHERE b."City" IS NULL AND b."Country" IS NULL
        ON CONFLICT ("City", "Country") DO NOTHING
        RETURNING *;        
    """.format(
        VALUES=data
    )
    return query


def query_insert_to_wind_data(data):
    query = """
        INSERT INTO "{TABLE}"("datetime", "CityId", "{DATATYPE}")
        SELECT a."datetime", a."CityId", a."{DATATYPE}"
        FROM (VALUES {VALUES}) AS a("datetime", "CityId", "{DATATYPE}")
        LEFT JOIN "{TABLE}" b
            ON a."datetime"=b."datetime" AND a."CityId"=b."CityId"
            WHERE b."datetime" IS NULL AND b."CityId" IS NULL
        ON CONFLICT ("datetime", "CityId") DO NOTHING
        RETURNING *;
    """.format(
        TABLE=data["table"],
        DATATYPE=data["datatype"],
        VALUES=data["values"]
    )
    #print(query)
    return query

def query_update_to_wind_data(data):
    query = """
        UPDATE "{TABLE}" AS t
        SET "{DATATYPE}"=c."{DATATYPE}"
        FROM (
            SELECT a."datetime", a."CityId", a."{DATATYPE}"
            FROM (VALUES {VALUES}) AS a("datetime", "CityId", "{DATATYPE}")
            INNER JOIN "{TABLE}" b
            ON (a."datetime"=b."datetime" AND a."CityId"=b."CityId")
            ) AS c("datetime", "CityId", "{DATATYPE}")
        WHERE c."datetime"=t."datetime" AND c."CityId"=t."CityId"
        ;
        INSERT INTO "{TABLE}"("datetime", "CityId", "{DATATYPE}")
        SELECT a."datetime", a."CityId", a."{DATATYPE}"
        FROM (VALUES {VALUES}) AS a("datetime", "CityId", "{DATATYPE}")
        LEFT JOIN "{TABLE}" b
            ON a."datetime"=b."datetime" AND a."CityId"=b."CityId"
            WHERE b."datetime" IS NULL AND b."CityId" IS NULL
        ON CONFLICT ("datetime", "CityId") DO NOTHING
        RETURNING *;
    """.format(
        TABLE=data["table"],
        DATATYPE=data["datatype"],
        VALUES=data["values"]
    )
    #print(query)
    return query




def query_insert_to_wind_data_simple_row(data):
    #print(data)
    query = """
        INSERT INTO "{TABLE}"("datetime", "CityId", "{DATATYPE}")
        VALUES {VALUES}
        ON CONFLICT ("datetime", "CityId") DO UPDATE SET "{DATATYPE}"=EXCLUDED."{DATATYPE}"
        RETURNING *;
        
    """.format(
        TABLE=data["table"],
        DATATYPE=data["datatype"],
        VALUES=data["values"]
    )
    #print(query)
    return query

def query_upsert_to_wind_data(data):
    query = """
    WITH
        n("datetime", "CityId", "{DATATYPE}") AS (
        VALUES {VALUES}
    ),

    upsert AS (
          UPDATE "{TABLE}" tbl
          SET "{DATATYPE}"=n."{DATATYPE}"
          FROM n WHERE tbl."datetime" = n."datetime" and tbl."CityId" = n."CityId"
          RETURNING (tbl."datetime", tbl."CityId")
        )

    INSERT INTO "{TABLE}" ("datetime", "CityId", "{DATATYPE}")
    SELECT n."datetime", n."CityId", n."{DATATYPE}"
    FROM n
    WHERE (n."datetime", n."CityId") NOT IN (
      SELECT "datetime", "CityId" FROM upsert
    )ON CONFLICT ("datetime", "CityId") DO UPDATE SET "{DATATYPE}"=EXCLUDED."{DATATYPE}"
        RETURNING *;
    """.format(
        TABLE=data["table"],
        DATATYPE=data["datatype"],
        VALUES=data["values"]
    )
    #print(query)
    return query

QUERIES = {
    "query_get_cities": query_get_cities,
    "query_get_cityid_by_city": query_get_cityid_by_city,
    "query_insert_to_city_attributes": query_insert_to_city_attributes,
    "query_update_to_city_attributes": query_update_to_city_attributes,
    "query_insert_to_wind_data": query_insert_to_wind_data,
    "query_update_to_wind_data": query_update_to_wind_data,
    "query_insert_to_wind_data_simple_row": query_insert_to_wind_data_simple_row,
    "query_upsert_to_wind_data": query_upsert_to_wind_data
}
