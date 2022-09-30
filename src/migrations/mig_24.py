upgrade = """
ALTER TABLE public.document ADD COLUMN name character varying;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN name;
"""
