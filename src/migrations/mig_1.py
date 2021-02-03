upgrade = """ALTER TABLE public.refresher
    ADD COLUMN valid boolean;"""

downgrade = """ALTER TABLE public.refresher
    DROP COLUMN valid;"""
