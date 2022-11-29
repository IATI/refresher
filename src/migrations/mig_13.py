upgrade = """
ALTER TABLE public.document ADD COLUMN lakify_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN lakify_end timestamp without time zone;
ALTER TABLE public.document ADD COLUMN lakify_error character varying;
"""
downgrade = """
    ALTER TABLE public.document
        DROP COLUMN lakify_start;

    ALTER TABLE public.document
        DROP COLUMN lakify_end;

    ALTER TABLE public.document
        DROP COLUMN lakify_error;
"""
