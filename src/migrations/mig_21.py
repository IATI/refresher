upgrade = """
ALTER TABLE public.document ADD COLUMN file_schema_valid BOOLEAN;
UPDATE public.document
SET file_schema_valid = public.validation.valid
FROM public.validation
WHERE document.validation = public.validation.id;
"""
downgrade = """
ALTER TABLE public.document
    DROP COLUMN file_schema_valid;
"""
