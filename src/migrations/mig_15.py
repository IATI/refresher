upgrade = """
ALTER TABLE public.validation ADD COLUMN publisher character varying;
UPDATE public.validation SET publisher = public.document.publisher FROM public.document where public.validation.document_id = document.id;
"""
downgrade = """
ALTER TABLE public.validation DROP COLUMN publisher;
"""
