upgrade = """
alter table public.document rename column bds_cache_url to cached_dataset_url;
"""
downgrade = """
alter table public.document rename column cached_dataset_url to bds_cache_url;
"""
