upgrade = """
ALTER TABLE public.document ADD COLUMN activity_level_validation timestamp without time zone;
ALTER TABLE public.publisher ADD COLUMN black_flag timestamp without time zone;
ALTER TABLE public.publisher ADD COLUMN black_flag_notified boolean;
"""
downgrade = """
ALTER TABLE public.document DROP COLUMN activity_level_validation;
ALTER TABLE public.publisher DROP COLUMN black_flag;
ALTER TABLE public.publisher DROP COLUMN black_flag_notified;
"""