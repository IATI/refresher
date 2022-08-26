upgrade = """
ALTER TABLE public.adhoc_validation ADD COLUMN session_id character varying;
CREATE INDEX index_session_id ON public.element_to_activity USING btree (element_key);
"""
downgrade = """
    ALTER TABLE public.adhoc_validation
        DROP COLUMN session_id;
"""
