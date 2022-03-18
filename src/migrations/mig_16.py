upgrade = """
ALTER TABLE public.document ADD COLUMN activity_level_validation timestamp without time zone;
ALTER TABLE publisher SET ADD COLUMN black_flag boolean;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN activity_level_validation;
ALTER TABLE publisher DROP COLUMN black_flag DEFAULT false;
"""