upgrade = """
ALTER TABLE public.document ADD COLUMN flatten_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN flatten_end timestamp without time zone;
ALTER TABLE public.document ADD COLUMN flatten_api_error integer;
ALTER TABLE public.document ADD COLUMN flattened_activities jsonb;
"""
downgrade = """
    ALTER TABLE public.document
        DROP COLUMN flatten_start;

    ALTER TABLE public.document
        DROP COLUMN flatten_end;

    ALTER TABLE public.document
        DROP COLUMN flatten_api_error;

    ALTER TABLE public.document
        DROP COLUMN flattened_activities;
"""
