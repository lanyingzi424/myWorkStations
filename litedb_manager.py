import sqlite3
from typing import List, Dict, Any, Optional


class LiteDBManager:
    """
    一个用于操作SQLite数据库（LiteDB）的Python类
    包含基本的增删改查功能
    """
    
    def __init__(self, db_path: str):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        self.connect()
    
    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()
            print(f"成功连接到数据库: {self.db_path}")
        except sqlite3.Error as e:
            print(f"连接数据库失败: {e}")
    
    def disconnect(self):
        """断开数据库连接"""
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭")
    
    def create_table(self, table_name: str, columns: Dict[str, str]) -> bool:
        """
        创建数据表
        
        Args:
            table_name: 表名
            columns: 列定义，格式为 {'列名': '数据类型'}
            
        Returns:
            bool: 操作是否成功
        """
        try:
            # 构建列定义字符串
            column_defs = []
            for col_name, col_type in columns.items():
                column_defs.append(f"{col_name} {col_type}")
            
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
            self.cursor.execute(sql)
            self.connection.commit()
            print(f"表 '{table_name}' 创建成功")
            return True
        except sqlite3.Error as e:
            print(f"创建表失败: {e}")
            return False
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> bool:
        """
        插入数据（增加）
        
        Args:
            table_name: 表名
            data: 要插入的数据，格式为 {'列名': '值'}
            
        Returns:
            bool: 操作是否成功
        """
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            values = list(data.values())
            
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(sql, values)
            self.connection.commit()
            print(f"数据插入成功，ID: {self.cursor.lastrowid}")
            return True
        except sqlite3.Error as e:
            print(f"插入数据失败: {e}")
            return False
    
    def bulk_insert(self, table_name: str, data_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入数据
        
        Args:
            table_name: 表名
            data_list: 要插入的数据列表
            
        Returns:
            bool: 操作是否成功
        """
        if not data_list:
            return True
        
        try:
            # 使用第一个字典的键作为列名
            columns = ', '.join(data_list[0].keys())
            placeholders = ', '.join(['?' for _ in data_list[0]])
            
            values_list = []
            for data in data_list:
                values_list.append(list(data.values()))
            
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.executemany(sql, values_list)
            self.connection.commit()
            print(f"批量插入成功，影响行数: {len(data_list)}")
            return True
        except sqlite3.Error as e:
            print(f"批量插入失败: {e}")
            return False
    
    def select(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
               columns: str = "*", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        查询数据（查找）
        
        Args:
            table_name: 表名
            conditions: 查询条件，格式为 {'列名': '值'}
            columns: 要查询的列，默认为所有列
            limit: 限制返回结果数量
            
        Returns:
            List[Dict]: 查询结果列表
        """
        try:
            sql = f"SELECT {columns} FROM {table_name}"
            params = []
            
            if conditions:
                where_clause = " AND ".join([f"{key} = ?" for key in conditions.keys()])
                sql += f" WHERE {where_clause}"
                params = list(conditions.values())
            
            if limit:
                sql += f" LIMIT {limit}"
            
            self.cursor.execute(sql, params)
            rows = self.cursor.fetchall()
            
            # 获取列名
            column_names = [description[0] for description in self.cursor.description]
            
            # 将结果转换为字典列表
            result = []
            for row in rows:
                result.append(dict(zip(column_names, row)))
            
            return result
        except sqlite3.Error as e:
            print(f"查询数据失败: {e}")
            return []
    
    def update(self, table_name: str, data: Dict[str, Any], 
               conditions: Dict[str, Any]) -> bool:
        """
        更新数据（修改）
        
        Args:
            table_name: 表名
            data: 要更新的数据，格式为 {'列名': '新值'}
            conditions: 更新条件，格式为 {'列名': '值'}
            
        Returns:
            bool: 操作是否成功
        """
        try:
            set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
            where_clause = " AND ".join([f"{key} = ?" for key in conditions.keys()])
            
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            params = list(data.values()) + list(conditions.values())
            
            self.cursor.execute(sql, params)
            self.connection.commit()
            print(f"数据更新成功，影响行数: {self.cursor.rowcount}")
            return True
        except sqlite3.Error as e:
            print(f"更新数据失败: {e}")
            return False
    
    def delete(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """
        删除数据（删除）
        
        Args:
            table_name: 表名
            conditions: 删除条件，格式为 {'列名': '值'}
            
        Returns:
            bool: 操作是否成功
        """
        try:
            where_clause = " AND ".join([f"{key} = ?" for key in conditions.keys()])
            
            sql = f"DELETE FROM {table_name} WHERE {where_clause}"
            params = list(conditions.values())
            
            self.cursor.execute(sql, params)
            self.connection.commit()
            print(f"数据删除成功，影响行数: {self.cursor.rowcount}")
            return True
        except sqlite3.Error as e:
            print(f"删除数据失败: {e}")
            return False
    
    def get_all_tables(self) -> List[str]:
        """
        获取数据库中所有表名
        
        Returns:
            List[str]: 表名列表
        """
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = self.cursor.fetchall()
            return [table[0] for table in tables if table[0] != 'sqlite_sequence']
        except sqlite3.Error as e:
            print(f"获取表名失败: {e}")
            return []
    
    def execute_sql(self, sql: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """
        执行自定义SQL语句
        
        Args:
            sql: SQL语句
            params: 参数列表
            
        Returns:
            List[Dict]: 查询结果
        """
        try:
            if params is None:
                params = []
            self.cursor.execute(sql, params)
            
            # 如果是查询语句，返回结果
            if sql.strip().upper().startswith('SELECT'):
                rows = self.cursor.fetchall()
                column_names = [description[0] for description in self.cursor.description]
                
                result = []
                for row in rows:
                    result.append(dict(zip(column_names, row)))
                
                return result
            else:
                # 如果是非查询语句，提交更改
                self.connection.commit()
                print("SQL语句执行成功")
                return []
        except sqlite3.Error as e:
            print(f"执行SQL失败: {e}")
            return []


# 示例使用
if __name__ == "__main__":
    # 创建数据库管理器实例
    db = LiteDBManager("example.db")
    
    # 创建表
    table_columns = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "age": "INTEGER",
        "email": "TEXT UNIQUE"
    }
    db.create_table("users", table_columns)
    
    # 插入数据
    user_data = {
        "name": "张三",
        "age": 28,
        "email": "zhangsan@example.com"
    }
    db.insert("users", user_data)
    
    # 批量插入
    users_data = [
        {"name": "李四", "age": 30, "email": "lisi@example.com"},
        {"name": "王五", "age": 25, "email": "wangwu@example.com"}
    ]
    db.bulk_insert("users", users_data)
    
    # 查询数据
    all_users = db.select("users")
    print("所有用户:", all_users)
    
    # 条件查询
    young_users = db.select("users", {"age": 25})
    print("年龄为25的用户:", young_users)
    
    # 更新数据
    db.update("users", {"age": 29}, {"name": "张三"})
    
    # 再次查询验证更新
    zhangsan = db.select("users", {"name": "张三"})
    print("更新后的张三信息:", zhangsan)
    
    # 删除数据
    db.delete("users", {"name": "王五"})
    
    # 最终查询
    remaining_users = db.select("users")
    print("剩余用户:", remaining_users)
    
    # 关闭连接
    db.disconnect()