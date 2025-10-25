    erDiagram
    USER ||--o{ PLAYER : "owns"

    USER {
    BIGINT   id PK "Primary Key"
    VARCHAR  email                 "UNIQUE, NOT NULL"
    TEXT     password              "NOT NULL"
    BOOLEAN  is_enabled            "Cuenta activa"
    BOOLEAN  account_no_expired    "Cuenta no expirada"
    BOOLEAN  account_no_locked     "Cuenta no bloqueada"
    BOOLEAN  credential_no_expired "Credencial no expirada"
    TIMESTAMP registration_date    "DEFAULT CURRENT_TIMESTAMP"
    TIMESTAMP last_login_date
    }

    PLAYER {  
    BIGINT   player_id PK                   "Primary Key"  
    BIGINT   user_id FK                     "Foreign Key → user.id"  
    VARCHAR  name                           "UNIQUE, NOT NULL"  

    DOUBLE   position_x                     "DEFAULT 0"
    DOUBLE   position_y                     "DEFAULT 0"
    DOUBLE   position_z                     "DEFAULT 0"

    INT      tile_x                         "0..31 derivado de x"
    INT      tile_y                         "0..31 derivado de y"
    INT      cx                             "chunk x"
    INT      cy                             "chunk y"
    VARCHAR  direction                      "ENUM('N','E','S','W')"

    BIGINT   class                          "NOT NULL"
    INT      coins                          "DEFAULT 0"
    INT      cp                             "DEFAULT 0"

    TIMESTAMP created_at                    "DEFAULT CURRENT_TIMESTAMP"
    TIMESTAMP updated_at
    TIMESTAMP deleted_at
    }