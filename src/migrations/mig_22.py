upgrade = """
ALTER TABLE public.document ADD COLUMN clean_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN clean_end timestamp without time zone;
ALTER TABLE public.document ADD COLUMN clean_error character varying;
ALTER TABLE public.document DROP COLUMN alv_start;
ALTER TABLE public.document DROP COLUMN alv_end;
ALTER TABLE public.document DROP COLUMN alv_error;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN clean_start;
ALTER TABLE public.document DROP COLUMN clean_end;
ALTER TABLE public.document DROP COLUMN clean_error;
ALTER TABLE public.document ADD COLUMN alv_end timestamp without time zone;
ALTER TABLE public.document ADD COLUMN alv_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN alv_error character varying;
"""
