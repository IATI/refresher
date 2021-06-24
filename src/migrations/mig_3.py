upgrade = """
CREATE TABLE public.validation (
    document_hash character varying PRIMARY KEY,
    document_id character varying NOT NULL,    
    document_url character varying NOT NULL,
    created timestamp without time zone NOT NULL,
    valid boolean NOT NULL,
    report jsonb NOT NULL
);

ALTER TABLE public.document ADD COLUMN validation character varying;

ALTER TABLE public.document DROP COLUMN valid;

ALTER TABLE ONLY public.document
    ADD CONSTRAINT related_validation FOREIGN KEY (validation) REFERENCES public.validation(document_hash);
"""

downgrade = """
    ALTER TABLE public.document
        DROP COLUMN validation;

    ALTER TABLE public.document ADD COLUMN valid boolean;

    DROP TABLE public.validation;
"""