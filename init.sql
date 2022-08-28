CREATE USER dwh_admin WITH PASSWORD 'password'; 

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dwh_admin;


CREATE TABLE public."user" (
    id integer NOT NULL,
    user_name varchar(64) UNIQUE NOT NULL,
    email varchar(64) UNIQUE NOT NULL,
    password_hash varchar(128) NOT NULL,
    registration_dt timestamp NOT NULL,
    last_login_dt timestamp NOT NULL,
    is_admin boolean not null,
    CONSTRAINT user_pk PRIMARY KEY (id)
);

CREATE TABLE public."list" (
    id integer NOT NULL,
    title varchar(64) NOT NULL,
    created_at timestamp NOT NULL,
    creator_id integer NOT null,
    CONSTRAINT list_pk PRIMARY KEY (id),
    CONSTRAINT fk_list_user
     foreign key (creator_id) 
     REFERENCES public.user (id)
);

CREATE TABLE public."todo" (
    id integer NOT NULL,
    description varchar(255) NOT NULL,
    created_at timestamp NOT NULL,
    finished_at timestamp NULL,
    is_finished boolean not NULL,
    creator_id integer NOT NULL,
    list_id integer NOT NULL,
    
    CONSTRAINT todo_pk PRIMARY KEY (id),
     CONSTRAINT fk_todo_user
     foreign key (creator_id)
     REFERENCES public.user (id),
     CONSTRAINT fk_todo_list
     foreign key (creator_id) 
     REFERENCES public.list (id)
);