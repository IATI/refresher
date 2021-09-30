upgrade = """
CREATE TABLE public.adhoc_validation (
    id SERIAL PRIMARY KEY,
    guid character varying,
    filename character varying,
    user_email character varying,
    created timestamp without time zone,
    validated timestamp without time zone,
    validation_api_error character varying,
    valid boolean,
    report jsonb
);
"""
downgrade = """
DROP TABLE public.adhoc_validation
"""