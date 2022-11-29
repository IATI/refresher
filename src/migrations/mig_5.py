upgrade = """
ALTER TABLE public.document
    DROP CONSTRAINT related_publisher;

ALTER TABLE ONLY public.document
    ADD CONSTRAINT related_publisher FOREIGN KEY (publisher) REFERENCES public.publisher(org_id) ON DELETE cascade;
"""

downgrade = """
ALTER TABLE public.document
    DROP CONSTRAINT related_publisher;

ALTER TABLE ONLY public.document
    ADD CONSTRAINT related_publisher FOREIGN KEY (publisher) REFERENCES public.publisher(org_id);
"""
