#STAGING TABLES
CREATE_STAGING_LISTINGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.staging_listings (
	id  BIGINT,
	listing_url  varchar(256) NULL,
	scrape_id  varchar(256) NULL,
	last_scraped  varchar(256) NULL,
	name  varchar(256) NULL,
	summary  varchar(256) NULL,
	description  varchar(256) NULL,
	experiences_offered  varchar(256) NULL,
	thumbnail_url  varchar(256) NULL,
	medium_url  varchar(256) NULL,
	picture_url  varchar(256) NULL,
	xl_picture_url  varchar(256) NULL,
	host_id				  BIGINT NULL,
	host_url              varchar(256) NULL,
	host_name             varchar(256) NULL,
	host_since            varchar(256) NULL,
	host_location         varchar(256) NULL,
	host_is_superhost     varchar(256) NULL,
	host_thumbnail_url    varchar(256) NULL,
	host_picture_url      varchar(256) NULL,
	host_neighbourhood    varchar(256) NULL,
	host_listings_count   REAL NULL,
	host_total_listings_count	REAL NULL,
	host_verifications    varchar(256) NULL,
	host_has_profile_pic  varchar(256) NULL,
	host_identity_verified   	varchar(256) NULL,
	street  varchar(256) NULL,
	neighbourhood  varchar(256) NULL,
	neighbourhood_cleansed  varchar(256) NULL,
	neighbourhood_group_cleansed  varchar(256) NULL,
	city  varchar(256) NULL,
	state  varchar(256) NULL,
	zipcode  varchar(256) NULL,
	market  varchar(256) NULL,
	smart_location  varchar(256) NULL,
	country_code  varchar(256) NULL,
	country  varchar(256) NULL,
	latitude  varchar(256) NULL,
	longitude  varchar(256) NULL,
	is_location_exact  varchar(256) NULL,
	property_type  varchar(256) NULL,
	room_type  varchar(256) NULL,
	accommodates  varchar(256) NULL,
	bathrooms  varchar(256) NULL,
	bedrooms  varchar(256) NULL,
	beds  varchar(256) NULL,
	bed_type  varchar(256) NULL,
	amenities  varchar(256) NULL,
	price  varchar(256) NULL,
	cleaning_fee  varchar(256) NULL,
	guests_included  varchar(256) NULL,
	extra_people  varchar(256) NULL,
	minimum_nights  varchar(256) NULL,
	maximum_nights  varchar(256) NULL,
	calendar_updated  varchar(256) NULL,
	has_availability  varchar(256) NULL,
	availability_30  varchar(256) NULL,
	availability_60  varchar(256) NULL,
	availability_90  varchar(256) NULL,
	availability_365  varchar(256) NULL,
	calendar_last_scraped  varchar(256) NULL,
	number_of_reviews  BIGINT NULL,
	first_review  varchar(256) NULL,
	last_review  varchar(256) NULL,
	review_scores_rating    DOUBLE PRECISION NULL,
	review_scores_accuracy  DOUBLE PRECISION NULL,
	review_scores_cleanliness  	DOUBLE PRECISION NULL,
	review_scores_checkin   DOUBLE PRECISION NULL,
	review_scores_communication	DOUBLE PRECISION NULL,
	review_scores_location  DOUBLE PRECISION NULL,
	review_scores_value     DOUBLE PRECISION NULL,
	requires_license  varchar(256) NULL,
	jurisdiction_names  varchar(256) NULL,
	instant_bookable  varchar(256) NULL,
	is_business_travel_ready  varchar(256) NULL,
	cancellation_policy  varchar(256) NULL,
	require_guest_profile_picture  varchar(256) NULL,
	require_guest_phone_verification  varchar(256) NULL,
	calculated_host_listings_count  varchar(256) NULL,
	reviews_per_month       DOUBLE PRECISION NULL
);
"""

CREATE_STAGING_CALENDAR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.staging_calendar (
	listing_id BIGINT NOT NULL,
	date VARCHAR(256) NOT NULL,
	available VARCHAR(256) NULL
);
"""

CREATE_STAGING_REVIEW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.staging_review (
	listing_id VARCHAR(256) NOT NULL,
	id varchar(256) NULL,
	date VARCHAR(256) NULL,
	reviewer_id varchar(256) NULL,
	reviewer_name VARCHAR(36) NULL
	
);
"""
# DIM AND FACT TABLES

CREATE_DIM_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.dim_review_table (
	reviewer_id             varchar NOT NULL,
	listing_id              BIGINT  NOT NULL,
	date                    varchar(256) NULL,
	reviewer_name           varchar(256) NULL,
	first_review            varchar(256) NULL,
	last_review             varchar(256) NULL,
	review_scores_rating    DOUBLE PRECISION NULL,
	review_scores_accuracy  DOUBLE PRECISION NULL,
	review_scores_cleanliness  	DOUBLE PRECISION NULL,
	review_scores_checkin   DOUBLE PRECISION NULL,
	review_scores_communication	DOUBLE PRECISION NULL,
	review_scores_location  DOUBLE PRECISION NULL,
	review_scores_value     DOUBLE PRECISION NULL,
	reviews_per_month       DOUBLE PRECISION NULL,
	number_of_reviews       BIGINT NULL,
	CONSTRAINT guest_pkey PRIMARY KEY (reviewer_id)	
);

CREATE TABLE IF NOT EXISTS public.dim_calendar_table (
	listing_id BIGINT NOT NULL,
	date VARCHAR(256) ,
	available varchar(256) NULL,	
	CONSTRAINT calendar_pkey PRIMARY KEY (listing_id)
);

CREATE TABLE IF NOT EXISTS public.dim_host_table (
	host_id				  varchar(256) NULL,
	host_url              varchar(256) NULL,
	host_name             varchar(256) NULL,
	host_since            varchar(256) NULL,
	host_location         varchar(256) NULL,
	host_is_superhost     varchar(256) NULL,
	host_thumbnail_url    varchar(256) NULL,
	host_picture_url      varchar(256) NULL,
	host_neighbourhood    varchar(256) NULL,
	host_listings_count   REAL NULL,
	host_total_listings_count	REAL NULL,
	host_verifications    varchar(256) NULL,
	host_has_profile_pic  varchar(256) NULL,
	host_identity_verified   	varchar(256) NULL,
CONSTRAINT host_pkey PRIMARY KEY (host_name)	
);

CREATE TABLE IF NOT EXISTS public.dim_lisiting_table (
    id                           BIGINT NOT NULL,
	listing_url                  varchar(256) NULL,
	scrape_id                    varchar(256) NULL,
	last_scraped                 varchar(256) NULL,
	name                         varchar(256) NULL,
	summary                      varchar(256) NULL,
	description                  varchar(256) NULL,
	experiences_offered          varchar(256) NULL,
	thumbnail_url                varchar(256) NULL,
	medium_url                   varchar(256) NULL,
	picture_url                  varchar(256) NULL,
	xl_picture_url               varchar(256) NULL,
	requires_license             varchar(256) NULL,
	instant_bookable             varchar(256) NULL,
	is_business_travel_ready     varchar(256) NULL,
	cancellation_policy          varchar(256) NULL,
	require_guest_profile_picture   	varchar(256) NULL,
	require_guest_phone_verification	varchar(256) NULL,
	calculated_host_listings_count  	varchar(256) NULL,
	CONSTRAINT listing_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS public.dim_property_table (
	listing_id BIGINT NULL,
	property_type	varchar(256) NULL,
	accommodates 	varchar(256) NULL,
	bathrooms    	varchar(256) NULL,
	bedrooms  varchar(256) NULL,
	beds      varchar(256) NULL,
	bed_type  varchar(256) NULL,
	amenities   	varchar(256) NULL,
	CONSTRAINT property_pkey PRIMARY KEY (listing_id)		
);
"""
#FACT TABLE
CREATE_FACT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.fact_price_table (
	listing_id   BIGINT NOT NULL,
	reviewer_id  varchar NOT NULL,
	calendar_id  varchar NOT NULL,
	host_id	BIGINT NOT NULL,
	price        varchar NOT NULL,
	cleaning_fee  varchar(256) NULL, 
	CONSTRAINT price_pkey PRIMARY KEY (listing_id)		
);
"""