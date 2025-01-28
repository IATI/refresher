upgrade = """
ALTER TABLE public.document ADD COLUMN bds_cache_url character varying NULL;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN bds_cache_url;
"""
