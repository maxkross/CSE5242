--
-- PostgreSQL database dump
--

-- Dumped from database version 10.6 (Ubuntu 10.6-1.pgdg16.04+1)
-- Dumped by pg_dump version 10.6 (Ubuntu 10.6-1.pgdg16.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: iteminfo; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.iteminfo (
    i_partkey integer,
    i_category integer
);


ALTER TABLE public.iteminfo OWNER TO postgres;

--
-- Name: lineitem; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.lineitem (
    l_orderkey integer,
    l_partkey integer,
    l_price double precision
);


ALTER TABLE public.lineitem OWNER TO postgres;

--
-- Name: order; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public."order" (
    o_orderkey integer,
    o_zip integer
);


ALTER TABLE public."order" OWNER TO postgres;

--
-- Data for Name: iteminfo; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.iteminfo (i_partkey, i_category) FROM stdin;
2	200
3	300
4	400
\.


--
-- Data for Name: lineitem; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.lineitem (l_orderkey, l_partkey, l_price) FROM stdin;
1	2	0.340000000000000024
2	4	3.45000000000000018
3	6	9.86999999999999922
4	8	0.989999999999999991
5	10	9.44999999999999929
\.


--
-- Data for Name: order; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public."order" (o_orderkey, o_zip) FROM stdin;
1	43202
2	43202
4	43201
\.


--
-- PostgreSQL database dump complete
--

