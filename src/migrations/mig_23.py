upgrade = """
ALTER TABLE public.document DROP COLUMN alv_revalidate;
"""
downgrade = """
ALTER TABLE public.document ADD COLUMN alv_revalidate BOOLEAN;
UPDATE public.document SET alv_revalidate = 'f';
ALTER TABLE public.document ALTER COLUMN alv_revalidate SET NOT NULL;
ALTER TABLE public.document ALTER COLUMN alv_revalidate SET DEFAULT FALSE;
"""
