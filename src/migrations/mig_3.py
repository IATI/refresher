upgrade = """
CREATE TABLE public.validation (
    id serial PRIMARY KEY,
    document_id character varying NOT NULL,
    document_hash character varying NOT NULL,
    document_url character varying NOT NULL,
    created timestamp without time zone NOT NULL,
    valid boolean NOT NULL,
    report jsonb NOT NULL
);

ALTER TABLE public.document ADD COLUMN validation integer;

ALTER TABLE public.document DROP COLUMN valid;

ALTER TABLE ONLY public.document
    ADD CONSTRAINT related_validation FOREIGN KEY (validation) REFERENCES public.validation(id);
"""

downgrade = """
    ALTER TABLE public.document
        DROP COLUMN validation;

    DROP TABLE public.validation;
"""