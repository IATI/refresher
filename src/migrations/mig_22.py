upgrade = """
ALTER TABLE public.document ADD COLUMN clean_start timestamp without time zone;
ALTER TABLE public.document ADD COLUMN clean_end timestamp without time zone;
ALTER TABLE public.document ADD COLUMN clean_error character varying;
UPDATE public.document
SET
    regenerate_validation_report = 't',
    solrize_reindex = 't',
    lakify_start = null,
    lakify_end = null,
    lakify_error = null,
    flatten_end = null,
    flatten_start = null,
    flattened_activities = null,
    flatten_api_error = null,
    downloaded = null,
    download_error = null
WHERE
    validation is not Null
    AND alv_end is not null;
UPDATE public.document
SET
    regenerate_validation_report = 't',
    downloaded = null,
    download_error = null
WHERE
    validation is not Null
    AND alv_end is null
    AND alv_error is not null;
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
