upgrade = """
ALTER TABLE public.document ALTER COLUMN solr_api_error TYPE text;
"""
downgrade = """
ALTER TABLE public.document ALTER COLUMN solr_api_error TYPE integer;
"""
