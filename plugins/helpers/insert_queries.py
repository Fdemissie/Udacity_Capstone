#INSERT data
price_fact_table_insert = """
        SELECT
                listing.id, 
                reviewer.reviewer_id, 
                calendar.listing_id,
                listing.host_id,
                listing.price, 
                listing.cleaning_fee
            FROM staging_listings listing
            LEFT JOIN staging_review reviewer
                ON listing.id = reviewer.listing_id
            LEFT JOIN staging_calendar calendar
                ON listing.id = calendar.listing_id
            WHERE listing.price IS NOT NULL 
            AND reviewer.reviewer_id IS NOT NULL 
            AND calendar.listing_id IS NOT NULL
            AND listing.id IS NOT NULL;

    """

dim_review_table_insert = """
        SELECT distinct 
                reviewer.reviewer_id, 
                reviewer.listing_id::bigint,
                reviewer.date, 
                reviewer.reviewer_name,            
                listing.first_review ,           
                listing.last_review  ,           
                listing.review_scores_rating ,   
                listing.review_scores_accuracy,  
                listing.review_scores_cleanliness  ,	
                listing.review_scores_checkin ,
                listing.review_scores_communication,
                listing.review_scores_location,  
                listing.review_scores_value  ,   
                listing.reviews_per_month,       
                listing.number_of_reviews      
        FROM staging_review reviewer
        INNER JOIN staging_listings listing
                ON listing.id = reviewer.listing_id;
    """

dim_property_table_insert = """
        SELECT distinct 
                    listing.id,	
                    listing.property_type,	
                    listing.accommodates, 	
                    listing.bathrooms,    	
                    listing.bedrooms,
                    listing.beds  ,   
                    listing.bed_type,  
                    listing.amenities   	
        FROM staging_listings listing;
    """

dim_calendar_table_insert = """
        SELECT distinct 
                    calendar.listing_id,
                    calendar.date,
                    calendar.available
        FROM staging_calendar calendar;
    """

dim_listing_table_insert = """
        SELECT 
                	listing.id                           ,
                    listing.listing_url                  ,
                    listing.scrape_id                    ,
                    listing.last_scraped                 ,
                    listing.name                         ,
                    listing.summary                      ,
                    listing.description                  ,
                    listing.experiences_offered          ,
                    listing.thumbnail_url                ,
                    listing.medium_url                   ,
                    listing.picture_url                  ,
                    listing.xl_picture_url               ,
                    listing.requires_license             ,
                    listing.instant_bookable             ,
                    listing.is_business_travel_ready     ,
                    listing.cancellation_policy          ,
                    listing.require_guest_profile_picture   ,
                    listing.require_guest_phone_verification,
                    listing.calculated_host_listings_count
        FROM staging_listings listing;
    """
dim_host_table_insert = """
        SELECT 
                    listing.host_id            ,
                    listing.host_url           ,
                    listing.host_name          ,
                    listing.host_since         ,
                    listing.host_location      ,
                    listing.host_is_superhost  ,
                    listing.host_thumbnail_url ,
                    listing.host_picture_url   ,
                    listing.host_neighbourhood ,
                    listing.host_listings_count,
                    listing.host_total_listings_count,
                    listing.host_verifications,
                    listing.host_has_profile_pic,
                    listing.host_identity_verified
        FROM staging_listings listing;
    """