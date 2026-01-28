CREATE TABLE IF NOT EXISTS warfarm.players (
    id uuid primary key default gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES warfarm.users(id),
    player_name VARCHAR(100) NOT NULL UNIQUE,
    last_x_position double precision,
    last_y_position double precision,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);