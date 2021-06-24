upgrade = """
ALTER TABLE public.document
    ADD COLUMN datastore_build_error character varying
"""

downgrade = """
ALTER TABLE public.document
    DROP COLUMN datastore_build_error
"""