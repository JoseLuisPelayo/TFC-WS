CREATE ROLE warfarm_admin LOGIN PASSWORD 'example' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE app_warfarm   LOGIN PASSWORD 'example' NOSUPERUSER NOCREATEDB NOCREATEROLE;

CREATE SCHEMA IF NOT EXISTS warfarm AUTHORIZATION warfarm_admin;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

ALTER ROLE warfarm_admin SET search_path = warfarm;
ALTER ROLE app_warfarm SET search_path = warfarm, public;

REVOKE CREATE ON SCHEMA public FROM public;

GRANT USAGE ON SCHEMA warfarm TO app_warfarm;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA warfarm TO app_warfarm;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA warfarm TO app_warfarm;

ALTER DEFAULT PRIVILEGES FOR USER warfarm_admin IN SCHEMA warfarm
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_warfarm;

ALTER DEFAULT PRIVILEGES FOR USER warfarm_admin IN SCHEMA warfarm
GRANT USAGE, SELECT ON SEQUENCES TO app_warfarm;

SET ROLE warfarm_admin;


--
-- create table warfarm.users (
--     id bigint generated always as identity primary key ,
--     is_enabled boolean not null,
--     account_no_expired boolean not null,
--     account_no_locked boolean not null,
--     credential_no_expired boolean not null
-- );
--
-- create table warfarm.roles (
--     id bigint generated always as identity primary key ,
--     role_name varchar(50) not null unique
-- );
--
-- create table warfarm.permissions (
--     id bigint generated always as identity primary key ,
--     name varchar(50) not null unique
-- );
--
-- create table warfarm.user_roles (
--     user_id bigint references warfarm.users(id),
--     role_id bigint references warfarm.roles(id)
-- );
--
-- create table warfarm.role_permissions (
--     permission_id bigint references warfarm.permissions(id),
--     role_id bigint references warfarm.roles(id)
-- );
--
