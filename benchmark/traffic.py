import os
import re
import glob

def parse_single_log_file(file_path):
    """解析单个日志文件，返回传输大小总和"""
    total_kb = 0.0
    send_count = 0
    
    # 正则表达式匹配 "send: 数字 KB" 格式
    send_pattern = re.compile(r'send:\s*([\d.]+)\s*KB', re.IGNORECASE)
    stop_pattern = re.compile(r'committed round move to 26', re.IGNORECASE)
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line_num, line in enumerate(file, 1):
                # 检查是否遇到停止条件
                if stop_pattern.search(line):
                    print(f"  在第 {line_num} 行遇到停止条件")
                    break
                
                # 查找发送数据
                matches = send_pattern.findall(line)
                for match in matches:
                    kb_size = float(match)
                    total_kb += kb_size
                    send_count += 1
                    print(f"  第 {line_num} 行: send: {kb_size} KB")
        
        print(f"  文件统计: {send_count} 次发送, 总计 {total_kb:.3f} KB")
        return total_kb, send_count
        
    except Exception as e:
        print(f"  错误: 无法读取文件 {file_path}: {e}")
        return 0.0, 0

def analyze_all_logs(directory_path="."):
    """分析目录下所有日志文件"""
    
    # 查找所有log文件
    log_patterns = [
        os.path.join(directory_path, "*.log"),
        os.path.join(directory_path, "*.txt"),
        os.path.join(directory_path, "**/*.log"),
        os.path.join(directory_path, "**/*.txt")
    ]
    
    log_files = []
    for pattern in log_patterns:
        log_files.extend(glob.glob(pattern, recursive=True))
    
    # 去重
    log_files = list(set(log_files))
    
    if not log_files:
        print(f"在目录 '{directory_path}' 中未找到日志文件")
        return
    
    print(f"找到 {len(log_files)} 个日志文件:")
    for f in log_files:
        print(f"  - {f}")
    print()
    
    # 统计变量
    total_kb_all_files = 0.0
    total_send_count = 0
    processed_files = 0
    
    # 处理每个文件
    for log_file in log_files:
        print(f"正在处理: {log_file}")
        file_kb, file_count = parse_single_log_file(log_file)
        
        if file_count > 0:
            total_kb_all_files += file_kb
            total_send_count += file_count
            processed_files += 1
        
        print()
    
    # 输出最终结果
    print("=" * 60)
    print("最终统计结果:")
    print(f"总传输大小: {total_kb_all_files/1024:.3f} MB")
    print("=" * 60)

if __name__ == "__main__":
    # 可以修改这里的路径，默认为当前目录
    log_directory = "."  # 当前目录
    # log_directory = "/path/to/your/logs"  # 或者指定具体路径
    
    print("日志传输大小统计器")
    print(f"分析目录: {os.path.abspath(log_directory)}")
    print()
    
    analyze_all_logs("logs")