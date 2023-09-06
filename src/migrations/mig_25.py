upgrade = """
ALTER TABLE public.document ADD COLUMN last_solrize_end timestamp without time zone;
UPDATE public.document SET last_solrize_end = solrize_end WHERE last_solrize_end IS NULL;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN last_solrize_end;
"""
