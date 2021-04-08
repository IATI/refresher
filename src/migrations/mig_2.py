upgrade = """
CREATE TABLE public.publisher (
    org_id character varying NOT NULL,
    description character varying,
	title character varying NOT NULL,
	name character varying NOT NULL,
	state character varying,
	image_url character varying,
	approval_status character varying,
    registry_created timestamp without time zone,
    created timestamp without time zone NOT NULL,
    last_modified timestamp without time zone
);

ALTER TABLE ONLY public.publisher
    ADD CONSTRAINT "Publisher_pkey" PRIMARY KEY (org_id);

ALTER TABLE public.refresher
    RENAME TO document;

ALTER TABLE public.document ADD COLUMN publisher character varying;

CREATE INDEX fki_publisher ON public.document USING btree (publisher);

ALTER TABLE ONLY public.document
    ADD CONSTRAINT related_publisher FOREIGN KEY (publisher) REFERENCES public.publisher(org_id);
"""

downgrade = """
    ALTER TABLE public.document
        DROP COLUMN publisher;

    DROP TABLE public.publisher;

    ALTER TABLE public.refresher
        RENAME TO public.document;
"""