upgrade = """
CREATE TABLE public.adhoc_validation (
    id SERIAL,  
    hash character varying PRIMARY KEY,
    user_email citext,
    created timestamp without time zone NOT NULL,
    validated timestamp without time zone NOT NULL,
    validation_api_error character varying,
    valid boolean,
    report jsonb
);
"""
downgrade = """
DROP TABLE public.adhoc_validation
"""