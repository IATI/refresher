upgrade = """
ALTER TABLE public.document ADD COLUMN alv_error character varying;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN alv_error
"""