upgrade = """
ALTER TABLE public.publisher ADD COLUMN package_count integer;
ALTER TABLE public.publisher ADD COLUMN iati_id character varying;
"""

downgrade = """
    ALTER TABLE public.publisher
        DROP COLUMN package_count;

    ALTER TABLE public.publisher
        DROP COLUMN iati_id;
"""
