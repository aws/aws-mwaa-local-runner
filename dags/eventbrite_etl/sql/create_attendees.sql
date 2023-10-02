CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.table }} (
        id                 VARCHAR(512),
        created            TIMESTAMP,
        changed            TIMESTAMP,
        ticket_class_id    VARCHAR(512),
        variant_id         VARCHAR(512),
        ticket_class_name  VARCHAR(512),
        checked_in         BOOLEAN,
        cancelled          BOOLEAN,
        refunded           BOOLEAN,
        status             VARCHAR(512),
        event_id           VARCHAR(512),
        order_id           VARCHAR(512),
        guestlist_id       VARCHAR(512),
        invited_by         VARCHAR(512),
        delivery_method    VARCHAR(512),
        profile_name       VARCHAR(512),
        profile_email      VARCHAR(512),
        profile_first_name VARCHAR(512),
        profile_last_name  VARCHAR(512),
        profile_prefix     VARCHAR(512),
        profile_suffix     VARCHAR(512),
        profile_age        INTEGER,
        profile_job_title  VARCHAR(512),
        profile_company    VARCHAR(512),
        profile_website    VARCHAR(512),
        profile_blog       VARCHAR(512),
        profile_gender     VARCHAR(512),
        profile_birth_date TIMESTAMP,
        profile_cell_phone VARCHAR(512),
        affiliate          VARCHAR(512),
        run_date           TIMESTAMP DEFAULT GETDATE(),
        PRIMARY KEY(id))
