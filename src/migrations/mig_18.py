upgrade = """
ALTER TABLE public.document RENAME COLUMN activity_level_validation TO alv_end;
ALTER TABLE public.document ADD COLUMN alv_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN alv_error character varying;
"""
downgrade = """
ALTER TABLE public.document RENAME COLUMN alv_end TO activity_level_validation;
ALTER TABLE public.document DROP COLUMN alv_start;
ALTER TABLE public.document DROP COLUMN alv_error;
"""
