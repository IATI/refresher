upgrade = """
    ALTER TABLE public.document DROP CONSTRAINT related_validation;
    ALTER TABLE public.validation DROP CONSTRAINT validation_pkey;
    ALTER TABLE public.validation ADD COLUMN id SERIAL PRIMARY KEY;
    ALTER TABLE public.document DROP COLUMN validation;
    ALTER TABLE public.document ADD COLUMN validation integer;
    ALTER TABLE ONLY public.document
        ADD CONSTRAINT related_validation FOREIGN KEY (validation) REFERENCES public.validation(id);
    UPDATE public.document SET validation = validation.id FROM public.validation WHERE document.hash = validation.document_hash;
"""

downgrade = """
    ALTER TABLE public.document DROP CONSTRAINT related_validation;
    ALTER TABLE public.validation DROP CONSTRAINT validation_pkey;
    DELETE FROM validation a USING validation b WHERE a.document_hash=b.document_hash AND a.ctid < b.ctid;
    ALTER TABLE validation ADD PRIMARY KEY (document_hash);
    ALTER TABLE public.document DROP COLUMN validation;
    ALTER TABLE public.document ADD COLUMN validation character varying;
    ALTER TABLE ONLY public.document
        ADD CONSTRAINT related_validation FOREIGN KEY (validation) REFERENCES public.validation(document_hash);
    UPDATE public.document SET validation = validation.document_hash FROM public.validation WHERE document.hash = validation.document_hash;
    ALTER TABLE validation DROP COLUMN id;
"""