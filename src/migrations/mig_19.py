upgrade = """
ALTER TABLE public.document ADD COLUMN solrize_reindex BOOLEAN;
UPDATE public.document SET solrize_reindex = 'f';
ALTER TABLE public.document ALTER COLUMN solrize_reindex SET NOT NULL;
ALTER TABLE public.document ALTER COLUMN solrize_reindex SET DEFAULT FALSE;
"""
downgrade = """
ALTER TABLE public.document
    DROP COLUMN solrize_reindex;
"""
