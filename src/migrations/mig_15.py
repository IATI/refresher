upgrade = """
ALTER TABLE public.validation ADD COLUMN publisher character varying;
UPDATE validation SET publisher = document.publisher FROM document where validation.document_id = document.id;
"""
downgrade = """
ALTER TABLE public.validation DROP COLUMN publisher;
"""