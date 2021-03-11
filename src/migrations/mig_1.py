upgrade = """
    INSERT INTO public.attribute_type VALUES ('owner-ref', 'string');
    INSERT INTO public.attribute_type VALUES ('owner-name', 'string');
"""

downgrade = """
    DELETE FROM public.attribute_type WHERE name = 'owner-ref';
    DELETE FROM public.attribute_type WHERE name = 'owner-name';
"""