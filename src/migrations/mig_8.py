upgrade = """
ALTER TABLE public.document ADD COLUMN solrize_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN solrize_end timestamp without time zone;
ALTER TABLE public.document ADD COLUMN solr_api_error integer;
"""
downgrade = """
    ALTER TABLE public.document
        DROP COLUMN solrize_start;

    ALTER TABLE public.document
        DROP COLUMN solrize_end;

    ALTER TABLE public.document
        DROP COLUMN solr_api_error;
"""