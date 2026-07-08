import { Pool } from 'pg';

export const pool = new Pool({
  host: 'localhost',
  port: 5433,
  database: 'poc_recurring',
  user: 'poc',
  password: 'poc123',
});
