
CREATE SEQUENCE "city_attributes_CityId_seq" INCREMENT 1 MINVALUE 1 MAXVALUE  2147483647 START 1;

CREATE TABLE "public"."city_attributes" (
    "CityId" integer DEFAULT nextval('"city_attributes_CityId_seq"') NOT NULL,
    "City" character varying(50) NOT NULL,
    "Country" character varying(50) NOT NULL,
    "Latitude" double precision,
    "Longitude" double precision,
    CONSTRAINT "city_attributes_CityId" PRIMARY KEY ("CityId"),
    CONSTRAINT "city_attributes_City_Country" UNIQUE ("City", "Country")
) WITH (oids = false);

CREATE TABLE "public"."wind_data" (
    "datetime" timestamp NOT NULL,
    "CityId" integer NOT NULL,
    "wind_direction" numeric,
    "wind_speed" numeric,
    "humidity" numeric,
    "pressure" double precision,
    "temperature" double precision,
    "weather_description" character varying(50),
    CONSTRAINT "wind_data_datetime_cityId" UNIQUE ("datetime", "CityId"),
    CONSTRAINT "wind_data_cityId_fkey" FOREIGN KEY ("CityId") REFERENCES city_attributes("CityId") ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE
) WITH (oids = false);



