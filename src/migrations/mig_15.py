upgrade = """
ALTER TABLE public.validation ADD COLUMN publisher character varying;
"""
downgrade = """
ALTER TABLE public.validation DROP COLUMN publisher;
"""