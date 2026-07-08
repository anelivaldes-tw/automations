import { Pool } from 'pg';
import * as fs from 'fs';
import * as path from 'path';

const pool = new Pool({
  host: 'localhost',
  port: 5433,
  database: 'poc_recurring',
  user: 'poc',
  password: 'poc123',
});

async function setup() {
  const schema = fs.readFileSync(path.join(__dirname, 'schema.sql'), 'utf-8');
  await pool.query(schema);
  console.log('✅ Database schema created');
  await pool.end();
}

setup().catch(console.error);
