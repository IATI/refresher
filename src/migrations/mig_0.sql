upgrade = """
--
-- PostgreSQL database dump
--

-- Dumped from database version 11.7 (Ubuntu 11.7-0ubuntu0.19.10.1)
-- Dumped by pg_dump version 12.5 (Ubuntu 12.5-0ubuntu0.20.04.1)

-- Started on 2020-12-21 16:27:11 GMT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 604 (class 1247 OID 16525)
-- Name: att_data_type; Type: TYPE; Schema: public; Owner: dds
--

CREATE TYPE public.att_data_type AS ENUM (
    'date',
    'string',
    'numeric',
    'boolean'
);


ALTER TYPE public.att_data_type OWNER TO dds;

SET default_tablespace = '';

--
-- TOC entry 199 (class 1259 OID 16550)
-- Name: attribute; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.attribute (
    md5_pk uuid NOT NULL,
    name character varying NOT NULL,
    date_value timestamp without time zone,
    string_value character varying,
    numeric_value numeric,
    boolean_value boolean
);


ALTER TABLE public.attribute OWNER TO dds;

--
-- TOC entry 198 (class 1259 OID 16533)
-- Name: attribute_type; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.attribute_type (
    name character varying NOT NULL,
    type public.att_data_type
);


ALTER TABLE public.attribute_type OWNER TO dds;

--
-- TOC entry 196 (class 1259 OID 16487)
-- Name: element; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.element (
    md5_pk uuid NOT NULL,
    name character varying NOT NULL,
    text_raw text,
    text_tokens tsvector,
    is_root boolean NOT NULL
);


ALTER TABLE public.element OWNER TO dds;

--
-- TOC entry 203 (class 1259 OID 319792)
-- Name: element_to_activity; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.element_to_activity (
    element_key uuid NOT NULL,
    activity_key uuid NOT NULL
);


ALTER TABLE public.element_to_activity OWNER TO dds;

--
-- TOC entry 200 (class 1259 OID 16571)
-- Name: element_to_attribute; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.element_to_attribute (
    element_key uuid NOT NULL,
    attribute_key uuid NOT NULL
);


ALTER TABLE public.element_to_attribute OWNER TO dds;

--
-- TOC entry 197 (class 1259 OID 16511)
-- Name: element_to_child; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.element_to_child (
    element_key uuid NOT NULL,
    child_key uuid NOT NULL
);


ALTER TABLE public.element_to_child OWNER TO dds;

--
-- TOC entry 201 (class 1259 OID 16584)
-- Name: element_to_parent; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.element_to_parent (
    element_key uuid NOT NULL,
    parent_key uuid NOT NULL
);


ALTER TABLE public.element_to_parent OWNER TO dds;

--
-- TOC entry 202 (class 1259 OID 319695)
-- Name: refresher; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.refresher (
    id character varying NOT NULL,
    hash character varying,
    url character varying,
    new boolean,
    modified boolean,
    stale boolean,
    error boolean,
    root_element_key uuid
);


ALTER TABLE public.refresher OWNER TO dds;

--
-- TOC entry 204 (class 1259 OID 319851)
-- Name: version; Type: TABLE; Schema: public; Owner: dds
--

CREATE TABLE public.version (
    number character varying NOT NULL,
    migration integer
);


ALTER TABLE public.version OWNER TO dds;

--
-- TOC entry 3011 (class 0 OID 16550)
-- Dependencies: 199
-- Data for Name: attribute; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.attribute (md5_pk, name, date_value, string_value, numeric_value, boolean_value) FROM stdin;
\.


--
-- TOC entry 3010 (class 0 OID 16533)
-- Dependencies: 198
-- Data for Name: attribute_type; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.attribute_type (name, type) FROM stdin;
version	string
generated-datetime	date
linked-data-default	string
type	string
default-currency	string
linked-data-uri	string
budget-not-provided	string
role	string
activity-id	string
crs-channel-code	string
code	string
vocabulary-uri	string
significance	string
provider-activity-id	string
receiver-activity-id	string
ref	string
srsName	string
iati-equivalent	string
indicator-uri	string
measure	string
status	string
currency	string
value-date	string
vocabulary	string
name	string
value	string
last-updated-datetime	date
iso-date	date
extraction-date	date
humanitarian	boolean
ascending	boolean
aggregation-status	boolean
attached	boolean
priority	boolean
hierarchy	numeric
percentage	numeric
level	numeric
year	numeric
rate-1	numeric
rate-2	numeric
phaseout-year	numeric
format	string
url	string
secondary-reporter	boolean
\.


--
-- TOC entry 3008 (class 0 OID 16487)
-- Dependencies: 196
-- Data for Name: element; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.element (md5_pk, name, text_raw, text_tokens, is_root) FROM stdin;
\.


--
-- TOC entry 3015 (class 0 OID 319792)
-- Dependencies: 203
-- Data for Name: element_to_activity; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.element_to_activity (element_key, activity_key) FROM stdin;
\.


--
-- TOC entry 3012 (class 0 OID 16571)
-- Dependencies: 200
-- Data for Name: element_to_attribute; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.element_to_attribute (element_key, attribute_key) FROM stdin;
\.


--
-- TOC entry 3009 (class 0 OID 16511)
-- Dependencies: 197
-- Data for Name: element_to_child; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.element_to_child (element_key, child_key) FROM stdin;
\.


--
-- TOC entry 3013 (class 0 OID 16584)
-- Dependencies: 201
-- Data for Name: element_to_parent; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.element_to_parent (element_key, parent_key) FROM stdin;
\.


--
-- TOC entry 3014 (class 0 OID 319695)
-- Dependencies: 202
-- Data for Name: refresher; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.refresher (id, hash, url, new, modified, stale, error, root_element_key) FROM stdin;
\.


--
-- TOC entry 3016 (class 0 OID 319851)
-- Dependencies: 204
-- Data for Name: version; Type: TABLE DATA; Schema: public; Owner: dds
--

COPY public.version (number, migration) FROM stdin;
\.


--
-- TOC entry 2848 (class 2606 OID 16494)
-- Name: element Element_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element
    ADD CONSTRAINT "Element_pkey" PRIMARY KEY (md5_pk);


--
-- TOC entry 2858 (class 2606 OID 16557)
-- Name: attribute attribute_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.attribute
    ADD CONSTRAINT attribute_pkey PRIMARY KEY (md5_pk);


--
-- TOC entry 2856 (class 2606 OID 16539)
-- Name: attribute_type attribute_type_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.attribute_type
    ADD CONSTRAINT attribute_type_pkey PRIMARY KEY (name);


--
-- TOC entry 2861 (class 2606 OID 16609)
-- Name: element_to_attribute e2a_compound_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_attribute
    ADD CONSTRAINT e2a_compound_pkey PRIMARY KEY (element_key, attribute_key);


--
-- TOC entry 2872 (class 2606 OID 319796)
-- Name: element_to_activity e2act_compound_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_activity
    ADD CONSTRAINT e2act_compound_pkey PRIMARY KEY (element_key, activity_key);


--
-- TOC entry 2852 (class 2606 OID 16607)
-- Name: element_to_child e2c_compound_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_child
    ADD CONSTRAINT e2c_compound_pkey PRIMARY KEY (element_key, child_key);


--
-- TOC entry 2865 (class 2606 OID 16605)
-- Name: element_to_parent e2p_compound_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_parent
    ADD CONSTRAINT e2p_compound_pkey PRIMARY KEY (element_key, parent_key);


--
-- TOC entry 2870 (class 2606 OID 319702)
-- Name: refresher refresher_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.refresher
    ADD CONSTRAINT refresher_pkey PRIMARY KEY (id);


--
-- TOC entry 2876 (class 2606 OID 319858)
-- Name: version version_pkey; Type: CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.version
    ADD CONSTRAINT version_pkey PRIMARY KEY (number);


--
-- TOC entry 2849 (class 1259 OID 278772)
-- Name: el_name; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX el_name ON public.element USING btree (name);


--
-- TOC entry 2862 (class 1259 OID 319710)
-- Name: fki_attribute_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_attribute_key ON public.element_to_attribute USING btree (attribute_key);


--
-- TOC entry 2873 (class 1259 OID 319808)
-- Name: fki_e2ct_activty_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_e2ct_activty_key ON public.element_to_activity USING btree (activity_key);


--
-- TOC entry 2874 (class 1259 OID 319807)
-- Name: fki_e2ct_element_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_e2ct_element_key ON public.element_to_activity USING btree (element_key);


--
-- TOC entry 2863 (class 1259 OID 319709)
-- Name: fki_element_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_element_key ON public.element_to_attribute USING btree (element_key);


--
-- TOC entry 2853 (class 1259 OID 319712)
-- Name: fki_etc_child_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_etc_child_key ON public.element_to_child USING btree (child_key);


--
-- TOC entry 2854 (class 1259 OID 319711)
-- Name: fki_etc_element_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_etc_element_key ON public.element_to_child USING btree (element_key);


--
-- TOC entry 2866 (class 1259 OID 319714)
-- Name: fki_etp_element_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_etp_element_key ON public.element_to_parent USING btree (element_key);


--
-- TOC entry 2867 (class 1259 OID 319713)
-- Name: fki_etp_parent_key; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_etp_parent_key ON public.element_to_parent USING btree (parent_key);


--
-- TOC entry 2868 (class 1259 OID 319708)
-- Name: fki_file_root_element; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX fki_file_root_element ON public.refresher USING btree (root_element_key);


--
-- TOC entry 2850 (class 1259 OID 16599)
-- Name: name; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX name ON public.element USING gin (text_tokens);


--
-- TOC entry 2859 (class 1259 OID 264935)
-- Name: str_val; Type: INDEX; Schema: public; Owner: dds
--

CREATE INDEX str_val ON public.attribute USING btree (string_value);


--
-- TOC entry 2881 (class 2606 OID 16579)
-- Name: element_to_attribute attribute; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_attribute
    ADD CONSTRAINT attribute FOREIGN KEY (attribute_key) REFERENCES public.attribute(md5_pk);


--
-- TOC entry 2879 (class 2606 OID 16558)
-- Name: attribute attribute_type; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.attribute
    ADD CONSTRAINT attribute_type FOREIGN KEY (name) REFERENCES public.attribute_type(name);


--
-- TOC entry 2885 (class 2606 OID 319797)
-- Name: element_to_activity e2act_activity_key; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_activity
    ADD CONSTRAINT e2act_activity_key FOREIGN KEY (element_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2886 (class 2606 OID 319802)
-- Name: element_to_activity e2ct_element_key; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_activity
    ADD CONSTRAINT e2ct_element_key FOREIGN KEY (element_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2880 (class 2606 OID 16574)
-- Name: element_to_attribute element; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_attribute
    ADD CONSTRAINT element FOREIGN KEY (element_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2878 (class 2606 OID 16519)
-- Name: element_to_child element_to_child_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_child
    ADD CONSTRAINT element_to_child_key_fkey FOREIGN KEY (child_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2877 (class 2606 OID 16514)
-- Name: element_to_child element_to_element_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_child
    ADD CONSTRAINT element_to_element_key_fkey FOREIGN KEY (element_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2882 (class 2606 OID 16587)
-- Name: element_to_parent element_to_element_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_parent
    ADD CONSTRAINT element_to_element_key_fkey FOREIGN KEY (element_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2883 (class 2606 OID 16592)
-- Name: element_to_parent element_to_parent_key_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.element_to_parent
    ADD CONSTRAINT element_to_parent_key_fkey FOREIGN KEY (parent_key) REFERENCES public.element(md5_pk);


--
-- TOC entry 2884 (class 2606 OID 319703)
-- Name: refresher file_root_element; Type: FK CONSTRAINT; Schema: public; Owner: dds
--

ALTER TABLE ONLY public.refresher
    ADD CONSTRAINT file_root_element FOREIGN KEY (root_element_key) REFERENCES public.element(md5_pk) NOT VALID;


-- Completed on 2020-12-21 16:27:11 GMT

--
-- PostgreSQL database dump complete
--

"""
