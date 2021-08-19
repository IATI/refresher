upgrade = """
ALTER TABLE public.document ADD COLUMN regenerate_validation_report BOOLEAN;
UPDATE public.document SET regenerate_validation_report = 'f';
ALTER TABLE public.document ALTER COLUMN regenerate_validation_report SET NOT NULL;
ALTER TABLE public.document ALTER COLUMN regenerate_validation_report SET DEFAULT FALSE;
"""
downgrade = """
ALTER TABLE public.document
    DROP COLUMN regenerate_validation_report;
"""