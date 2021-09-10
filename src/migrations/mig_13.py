upgrade = """
ALTER TABLE public.publisher ADD COLUMN type_code integer;
ALTER TABLE public.publisher ADD COLUMN contact character varying;
ALTER TABLE public.publisher ADD COLUMN contact_email character varying;
ALTER TABLE public.publisher ADD COLUMN first_publish_date character varying;
"""

# Wanted to make first_publish_date a timestamp, but date format varies too widely

downgrade = """
    ALTER TABLE public.publisher
        DROP COLUMN type_code;

    ALTER TABLE public.publisher
        DROP COLUMN contact;

    ALTER TABLE public.publisher
        DROP COLUMN contact_email;

    ALTER TABLE public.publisher
        DROP COLUMN first_publish_date;
"""