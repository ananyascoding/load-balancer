================================================================
  DB LOAD BALANCER SIMULATOR — COMPLETE SETUP GUIDE
  (Mac · MySQL already installed via Homebrew · Java beginner)
================================================================

WHAT THIS PROJECT DOES
-----------------------
This is a Java web app that sits in front of 3 MySQL databases
and balances SELECT query requests across them. You open a
browser, type a SQL query, pick a balancing algorithm, and watch
which database node handles it — with live stats and a log table.


================================================================
PHASE 1 — INSTALL JAVA & MAVEN
================================================================

Open the Terminal app on your Mac (Spotlight → type "Terminal").

Run these commands one by one. Press Enter after each.

  brew install openjdk@17

  echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> ~/.zshrc
  echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

  source ~/.zshrc

  brew install maven

CHECK it worked:
  java -version        ← should say: openjdk 17...
  mvn -version         ← should say: Apache Maven 3...


================================================================
PHASE 2 — SET UP 3 MYSQL INSTANCES (different ports)
================================================================

Your existing MySQL already runs on port 3306.
We need 2 more instances on ports 3307 and 3308.

---- STEP 2A: Find where mysqld lives ----

  which mysqld
  # OR try:
  ls /opt/homebrew/bin/mysqld
  ls /usr/local/bin/mysqld

  NOTE the full path — you'll use it below.
  Example paths:
    Apple Silicon Mac → /opt/homebrew/bin/mysqld
    Intel Mac        → /usr/local/bin/mysqld

---- STEP 2B: Create data directories ----

  sudo mkdir -p /usr/local/mysql-instances/node2/data
  sudo mkdir -p /usr/local/mysql-instances/node3/data
  sudo chown -R $(whoami) /usr/local/mysql-instances

---- STEP 2C: Initialize nodes 2 and 3 ----

  (Replace /opt/homebrew/bin/mysqld with YOUR path from 2A)

  /opt/homebrew/bin/mysqld --initialize-insecure \
    --datadir=/usr/local/mysql-instances/node2/data \
    --user=$(whoami)

  /opt/homebrew/bin/mysqld --initialize-insecure \
    --datadir=/usr/local/mysql-instances/node3/data \
    --user=$(whoami)

  (These commands take a few seconds and print nothing on success)

---- STEP 2D: Create config files ----

  cat > /usr/local/mysql-instances/node2.cnf << 'EOF'
[mysqld]
port     = 3307
datadir  = /usr/local/mysql-instances/node2/data
socket   = /tmp/mysql_node2.sock
pid-file = /tmp/mysql_node2.pid
EOF

  cat > /usr/local/mysql-instances/node3.cnf << 'EOF'
[mysqld]
port     = 3308
datadir  = /usr/local/mysql-instances/node3/data
socket   = /tmp/mysql_node3.sock
pid-file = /tmp/mysql_node3.pid
EOF

---- STEP 2E: Start all 3 MySQL instances ----

  # Start node 1 (your existing MySQL)
  brew services start mysql

  # Start node 2  (replace mysqld path if needed)
  /opt/homebrew/bin/mysqld \
    --defaults-file=/usr/local/mysql-instances/node2.cnf \
    --daemonize

  # Start node 3
  /opt/homebrew/bin/mysqld \
    --defaults-file=/usr/local/mysql-instances/node3.cnf \
    --daemonize

  # Verify all 3 are running:
  lsof -i :3306 -i :3307 -i :3308 | grep LISTEN
  # You should see 3 lines with mysqld

---- STEP 2F: Create the database and table on all 3 nodes ----

  # Node 1 (port 3306 — your existing MySQL with password)
  mysql -u root -pStar@123 -P 3306 -h 127.0.0.1 << 'EOF'
CREATE DATABASE IF NOT EXISTS lbsim;
USE lbsim;
CREATE TABLE IF NOT EXISTS employees (
  id     INT PRIMARY KEY AUTO_INCREMENT,
  name   VARCHAR(100),
  dept   VARCHAR(50),
  salary DECIMAL(10,2)
);
INSERT INTO employees (name, dept, salary) VALUES
  ('Alice','Engineering',95000),('Bob','HR',62000),
  ('Carol','Finance',78000),('Dave','Engineering',101000),
  ('Eve','Marketing',71000);
EOF

  # Node 2 (port 3307 — no password yet)
  mysql -u root --socket=/tmp/mysql_node2.sock << 'EOF'
ALTER USER 'root'@'localhost' IDENTIFIED BY 'Star@123';
FLUSH PRIVILEGES;
CREATE DATABASE lbsim;
USE lbsim;
CREATE TABLE IF NOT EXISTS employees (
  id     INT PRIMARY KEY AUTO_INCREMENT,
  name   VARCHAR(100),
  dept   VARCHAR(50),
  salary DECIMAL(10,2)
);
INSERT INTO employees (name, dept, salary) VALUES
  ('Frank','Sales',68000),('Grace','Engineering',97000),
  ('Hank','Finance',82000),('Iris','HR',59000),
  ('Jack','Marketing',74000);
EOF

  # Node 3 (port 3308 — no password yet)
  mysql -u root --socket=/tmp/mysql_node3.sock << 'EOF'
ALTER USER 'root'@'localhost' IDENTIFIED BY 'Star@123';
FLUSH PRIVILEGES;
CREATE DATABASE lbsim;
USE lbsim;
CREATE TABLE IF NOT EXISTS employees (
  id     INT PRIMARY KEY AUTO_INCREMENT,
  name   VARCHAR(100),
  dept   VARCHAR(50),
  salary DECIMAL(10,2)
);
INSERT INTO employees (name, dept, salary) VALUES
  ('Karen','Engineering',99000),('Leo','Finance',76000),
  ('Mia','Sales',65000),('Ned','HR',61000),
  ('Olive','Marketing',73000);
EOF


================================================================
PHASE 3 — BUILD THE JAVA PROJECT
================================================================

---- STEP 3A: Move the project folder ----

  Move the downloaded "load-balancer-sim" folder to your Desktop.

---- STEP 3B: Open Terminal in that folder ----

  cd ~/Desktop/load-balancer-sim

---- STEP 3C: Build the project ----

  mvn clean package -DskipTests

  This downloads libraries from the internet (~2 minutes first time).
  You will see a lot of text scroll by. Wait for:

    BUILD SUCCESS

  If you see BUILD FAILURE, copy the red error and ask ChatGPT
  (use the prompt in the file CHATGPT_PROMPT.txt).

---- STEP 3D: Run the app ----

  java -jar target/load-balancer-sim-1.0.0.jar

  Wait until you see:
    Started LoadBalancerApp in X.XXX seconds

  Leave this Terminal window open while using the app.


================================================================
PHASE 4 — USE THE DASHBOARD
================================================================

Open your browser and go to:
  http://localhost:8080

You will see the Load Balancer Dashboard.

WHAT TO DO:
  1. The SQL input already has:  SELECT * FROM employees
  2. Pick an algorithm from the dropdown
  3. Click ▶ Send  — results appear below, log updates
  4. Click ⚡ Burst ×15 — fires 15 requests at once, watch the
     load distribute across nodes in the cards
  5. Switch algorithms and repeat to compare behaviour
  6. Click ↺ Reset to clear stats

ALGORITHMS EXPLAINED:
  Round Robin        — sends to Node1, Node2, Node3, Node1...
  Least Connections  — sends to whichever node is least busy
  Weighted Round RR  — same as Round Robin here (equal weights)

TRY THESE QUERIES:
  SELECT * FROM employees
  SELECT * FROM employees WHERE dept = 'Engineering'
  SELECT name, salary FROM employees ORDER BY salary DESC
  SELECT dept, COUNT(*) as total FROM employees GROUP BY dept


================================================================
PHASE 5 — STOP EVERYTHING
================================================================

  Stop the Java app:
    Press Ctrl+C in the Terminal window running the app

  Stop MySQL nodes 2 and 3:
    kill $(cat /tmp/mysql_node2.pid)
    kill $(cat /tmp/mysql_node3.pid)

  Stop MySQL node 1:
    brew services stop mysql


================================================================
RESTARTING NEXT TIME
================================================================

  # Start MySQL instances
  brew services start mysql
  /opt/homebrew/bin/mysqld --defaults-file=/usr/local/mysql-instances/node2.cnf --daemonize
  /opt/homebrew/bin/mysqld --defaults-file=/usr/local/mysql-instances/node3.cnf --daemonize

  # Start the app
  cd ~/Desktop/load-balancer-sim
  java -jar target/load-balancer-sim-1.0.0.jar

  # Open browser
  http://localhost:8080


================================================================
COMMON ERRORS & FIXES
================================================================

ERROR: "command not found: java"
  FIX: Run  source ~/.zshrc  then try again

ERROR: "mysqld: command not found"
  FIX: Find the correct path with  which mysqld
       Replace /opt/homebrew/bin/mysqld with that path

ERROR: "Access denied for user 'root'"
  FIX: Double-check your password is Star@123
       Check application.properties has the right password

ERROR: "Address already in use: 3307"
  FIX: A previous MySQL is still running.
       Run: kill $(cat /tmp/mysql_node2.pid)
       Then start it again.

ERROR: "BUILD FAILURE" in Maven
  FIX: Make sure you are in the right folder:
       cd ~/Desktop/load-balancer-sim
       Then run mvn clean package -DskipTests again

ERROR: Port 8080 already in use
  FIX: Something else is using 8080. Kill it:
       lsof -ti:8080 | xargs kill -9
       Then restart the jar.

================================================================
RERUN

# 1. Sync all 3 MySQL nodes (run once)
for PORT in 3306 3307 3308; do
  mysql -u root -pStar@123 -P $PORT -h 127.0.0.1 -e "
    USE lbsim;
    TRUNCATE TABLE employees;
    INSERT INTO employees (id,name,dept,salary) VALUES
      (1,'Alice','Engineering',95000),(2,'Bob','HR',62000),
      (3,'Carol','Finance',78000),(4,'Dave','Engineering',101000),
      (5,'Eve','Marketing',71000);
    ALTER TABLE employees AUTO_INCREMENT=6;"
done

# 2. Build and run
cd ~/Desktop/load-balancer-sim-v2
mvn clean package -DskipTests
java -jar target/load-balancer-sim-3.0.0.jar


#3. For checking in terminal updates in all 3 databases!

for PORT in 3306 3307 3308; do
  echo "PORT $PORT"
  mysql -u root -pStar@123 -P $PORT -h 127.0.0.1 -e "SELECT * FROM lbsim.employees;"
done