import warnings
# 忽略pandas的FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

import streamlit as st
import boto3
import pandas as pd
from datetime import datetime, timedelta
import io
import json
from io import BytesIO
import os
import shutil

# Connect API 函数
def initialize_clients(session, region):
    """初始化 AWS 客户端"""
    try:
        connect_client = session.client('connect', region_name=region)
        s3_client = session.client('s3', region_name=region)
        return connect_client, s3_client
    except Exception as e:
        st.error(f"无法初始化AWS客户端: {e}")
        st.stop()

def list_phone_numbers(connect_client, instance_id, max_results=100, next_token=None):
    """获取 Connect 实例的电话号码列表"""
    params = {
        'InstanceId': instance_id,
        'MaxResults': max_results
    }
    
    if next_token:
        params['NextToken'] = next_token
        
    return connect_client.list_phone_numbers(**params)

def get_all_phone_numbers(connect_client, instance_id):
    """获取所有电话号码，处理分页"""
    phone_numbers = []
    
    # 获取第一页结果
    response = list_phone_numbers(connect_client, instance_id)
    
    for number in response.get('PhoneNumberSummaryList', []):
        phone_numbers.append({
            'PhoneNumber': number.get('PhoneNumber'),
            'PhoneNumberId': number.get('PhoneNumberId'),
            'PhoneNumberType': number.get('PhoneNumberType')
        })
    
    # 处理分页
    while 'NextToken' in response:
        response = list_phone_numbers(
            connect_client, 
            instance_id, 
            next_token=response['NextToken']
        )
        
        for number in response.get('PhoneNumberSummaryList', []):
            phone_numbers.append({
                'PhoneNumber': number.get('PhoneNumber'),
                'PhoneNumberId': number.get('PhoneNumberId'),
                'PhoneNumberType': number.get('PhoneNumberType')
            })
    
    return phone_numbers

def get_call_recordings_s3_bucket(connect_client, instance_id, association_id):
    """
    获取 Amazon Connect 实例的通话录音 S3 存储桶路径
    
    :param connect_client: Connect 客户端
    :param instance_id: Amazon Connect 实例 ID
    :param association_id: 关联 ID
    :return: 通话录音的 S3 存储桶路径
    """
    try:
        # 获取实例存储配置
        response = connect_client.describe_instance_storage_config(
            InstanceId=instance_id,
            AssociationId=association_id,
            ResourceType='CALL_RECORDINGS'  # 通话录音的固定值
        )
        
        # 从存储配置中提取 S3 存储桶路径
        storage_config = response.get('StorageConfig', {})
        s3_config = storage_config.get('S3Config', {})
        
        if s3_config:
            bucket_name = s3_config.get('BucketName')
            bucket_prefix = s3_config.get('BucketPrefix', '')
            
            if bucket_name:
                return f"s3://{bucket_name}/{bucket_prefix}"
            else:
                return "S3Config 中缺少 BucketName"
        else:
            return "StorageConfig 中缺少 S3Config"
    
    except Exception as e:
        return f"获取通话录音 S3 路径时出错: {str(e)}"

def get_call_recordings_list(s3_client, s3_bucket_path, phone_numbers=None):
    """
    获取指定 S3 存储桶路径中的所有通话录音列表
    
    :param s3_client: S3 客户端
    :param s3_bucket_path: S3 存储桶路径，格式为 's3://{bucket_name}/{bucket_prefix}'
    :param phone_numbers: 可选，筛选特定电话号码的录音
    :return: 包含录音信息的列表
    """
    try:
        # 解析 S3 存储桶路径
        if not s3_bucket_path.startswith('s3://'):
            raise ValueError("S3 存储桶路径格式无效，应以 's3://' 开头")
        
        parts = s3_bucket_path.replace('s3://', '').split('/', 1)
        bucket_name = parts[0]
        bucket_prefix = parts[1] if len(parts) > 1 else ''
        
        # 列出 S3 存储桶中的对象
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=bucket_prefix)
        
        recordings = []
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # 检查对象是否为录音文件（假设为 .wav 格式）
                    if key.endswith('.wav'):
                        # 从文件路径中提取联系 ID
                        file_name = key.split('/')[-1]
                        contact_id = file_name.split('_')[0] if '_' in file_name else file_name.replace('.wav', '')
                        
                        # 默认使用第一个选择的电话号码
                        default_phone = phone_numbers[0] if phone_numbers else '未知'
                        
                        # 构建录音信息
                        recording_info = {
                            'ContactId': contact_id,
                            '录音S3地址': f"s3://{bucket_name}/{key}",
                            '热线号码': default_phone  # 默认使用第一个选择的号码
                        }
                        
                        # 如果有电话号码筛选，尝试从文件名或路径中提取
                        if phone_numbers and len(phone_numbers) > 1:
                            for phone in phone_numbers:
                                if phone in key:
                                    recording_info['热线号码'] = phone
                                    break
                        
                        recordings.append(recording_info)
        
        return recordings
    
    except Exception as e:
        st.error(f"获取录音列表时出错: {str(e)}")
        return []

def get_contact_files_list(s3_client, s3_bucket_path):
    """
    获取指定 S3 存储桶路径中的所有联系记录文件
    
    :param s3_client: S3 客户端
    :param s3_bucket_path: S3 存储桶路径，格式为 's3://{bucket_name}/{bucket_prefix}'
    :return: 包含联系记录信息的列表
    """
    try:
        # 解析 S3 存储桶路径
        if not s3_bucket_path.startswith('s3://'):
            raise ValueError("S3 存储桶路径格式无效，应以 's3://' 开头")
        
        parts = s3_bucket_path.replace('s3://', '').split('/', 1)
        bucket_name = parts[0]
        bucket_prefix = parts[1] if len(parts) > 1 else ''
        
        # 列出 S3 存储桶中的对象
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=bucket_prefix)
        
        contact_files = []
        processed_files = 0
        max_files = 1000  # 限制处理的文件数量，避免处理太多文件
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # 检查对象是否为 CSV 或 Parquet 文件
                    if key.endswith('.csv') or key.endswith('.parquet'):
                        processed_files += 1
                        if processed_files > max_files:
                            st.warning(f"已达到最大处理文件数量限制 ({max_files})，停止处理更多文件")
                            return contact_files
                        
                        try:
                            # 获取对象
                            s3_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                            file_content = s3_obj['Body'].read()
                            
                            # 根据文件类型解析
                            if key.endswith('.csv'):
                                df = pd.read_csv(BytesIO(file_content))
                            elif key.endswith('.parquet'):
                                df = pd.read_parquet(BytesIO(file_content))
                            
                            # 提取联系 ID 和地址值
                            if 'contactid' in df.columns:
                                for _, row in df.iterrows():
                                    contact_info = {
                                        'ContactId': row.get('contactid', ''),
                                        '热线号码': '',
                                        '客户号码': '',
                                        '文件路径': key
                                    }
                                    
                                    # 尝试从不同字段获取电话号码
                                    if 'systemendpoint' in df.columns:
                                        if isinstance(row.get('systemendpoint'), dict):
                                            contact_info['热线号码'] = row['systemendpoint'].get('address', '')
                                        elif isinstance(row.get('systemendpoint'), str):
                                            try:
                                                system_endpoint = json.loads(row['systemendpoint'])
                                                contact_info['热线号码'] = system_endpoint.get('address', '')
                                            except:
                                                pass
                                    
                                    if not contact_info['客户号码'] and 'customerendpoint' in df.columns:
                                        if isinstance(row.get('customerendpoint'), dict):
                                            contact_info['客户号码'] = row['customerendpoint'].get('address', '')
                                        elif isinstance(row.get('customerendpoint'), str):
                                            try:
                                                customer_endpoint = json.loads(row['customerendpoint'])
                                                contact_info['客户号码'] = customer_endpoint.get('address', '')
                                            except:
                                                pass
                                    
                                    contact_files.append(contact_info)
                        except Exception as e:
                            st.warning(f"处理文件 {key} 时出错: {str(e)}")
                            continue
        
        return contact_files
    
    except Exception as e:
        st.error(f"获取通话列表时出错: {str(e)}")
        return []

def merge_contacts_and_recordings(contact_files, recordings, selected_numbers=None):
    """
    合并联系记录和录音记录，基于ContactId进行left join
    
    :param contact_files: 联系记录列表
    :param recordings: 录音记录列表
    :param selected_numbers: 可选，筛选特定电话号码的记录
    :return: 合并后的记录列表
    """
    try:
        # 转换为DataFrame
        df_contacts = pd.DataFrame(contact_files)
        df_recordings = pd.DataFrame(recordings)
        
        # 确保两个DataFrame都有ContactId列
        if 'ContactId' not in df_contacts.columns or 'ContactId' not in df_recordings.columns:
            st.warning("联系记录或录音记录缺少ContactId列，无法合并")
            return []
        
        # 执行left join
        merged_df = pd.merge(
            df_contacts, 
            df_recordings, 
            on='ContactId', 
            how='left', 
            suffixes=('', '_recording')
        )
        
        # 筛选特定电话号码的记录
        if selected_numbers and len(selected_numbers) > 0:
            # 创建筛选条件：热线号码在selected_numbers中
            mask = merged_df['热线号码'].isin(selected_numbers)
            merged_df = merged_df[mask]
        
        # 添加是否有录音的标记
        merged_df['有录音'] = merged_df['录音S3地址'].notna()
        
        return merged_df.to_dict('records')
    
    except Exception as e:
        st.error(f"合并联系记录和录音记录时出错: {str(e)}")
        return []

def download_recordings_to_directories(s3_client, merged_records, recording_dir):
    """
    将录音文件下载到按热线号码组织的目录中
    
    :param s3_client: S3客户端
    :param merged_records: 合并后的记录列表
    :param base_dir: 基础目录，如果为None则使用当前目录
    :return: 下载统计信息
    """
    try:
        # 如果没有指定基础目录，则使用当前目录
        base_dir = os.getcwd()
        
        # 创建recordings目录作为根目录
        recordings_dir = os.path.join(base_dir, recording_dir)
        os.makedirs(recordings_dir, exist_ok=True)
        
        print(recording_dir)
        
        # 记录下载的文件数量
        downloaded_count = 0
        failed_count = 0
        
        # 按热线号码分组
        phone_groups = {}
        for record in merged_records:
            s3_uri = record.get('录音S3地址')
            if not s3_uri or pd.isna(s3_uri):
                continue
                
            phone_number = record.get('热线号码', '未知')
            if phone_number not in phone_groups:
                phone_groups[phone_number] = []
            
            phone_groups[phone_number].append(record)
        
        print(phone_groups)
        
        # 为每个热线号码创建目录并下载文件
        for phone_number, records in phone_groups.items():
            # 创建目录名称（去掉+号）
            dir_name = phone_number.replace('+', '')
            dir_path = os.path.join(recordings_dir, dir_name)
            
            # 创建目录
            os.makedirs(dir_path, exist_ok=True)
            st.write(f"创建目录: {dir_path}")
            
            # 下载该热线号码的所有录音
            for record in records:
                s3_uri = record.get('录音S3地址')
                contact_id = record.get('ContactId', '')
                
                try:
                    # 解析S3 URI
                    s3_parts = s3_uri.replace('s3://', '').split('/')
                    bucket_name = s3_parts[0]
                    object_key = '/'.join(s3_parts[1:])
                    
                    # 文件名称
                    file_name = f"{contact_id}.wav"
                    file_path = os.path.join(dir_path, file_name)
                    
                    # 下载文件
                    s3_client.download_file(bucket_name, object_key, file_path)
                    downloaded_count += 1
                    
                    st.write(f"下载文件: {file_path}")
                except Exception as e:
                    st.warning(f"下载录音失败 (ContactId: {contact_id}): {e}")
                    failed_count += 1
        
        return {
            "base_dir": recordings_dir,
            "downloaded": downloaded_count,
            "failed": failed_count,
            "phone_groups": len(phone_groups)
        }
    
    except Exception as e:
        st.error(f"下载全部录音失败: {e}")
        return None

# S3 API 函数
def get_s3_object(s3_client, s3_uri):
    """从S3获取对象"""
    # 解析S3 URI
    s3_parts = s3_uri.replace('s3://', '').split('/')
    bucket_name = s3_parts[0]
    object_key = '/'.join(s3_parts[1:])
    
    # 获取文件
    return s3_client.get_object(Bucket=bucket_name, Key=object_key)

def download_recording(s3_client, s3_uri, phone_number, contact_id):
    """下载录音文件并创建下载按钮"""
    try:
        obj = get_s3_object(s3_client, s3_uri)
        data = obj['Body'].read()
        
        # 文件名称
        file_name = f"recording_{phone_number}_{contact_id}.wav"
        
        # 提供下载
        st.download_button(
            label=f"下载 {file_name}",
            data=data,
            file_name=file_name,
            mime="audio/wav"
        )
    except Exception as e:
        st.error(f"下载录音失败: {e}")

def extract_number_after_plus(selected_numbers):
    """
    从selected_numbers中的第一个号码中提取"+"后面的数字
    
    :param selected_numbers: 电话号码列表
    :return: "+"后面的数字，如果没有"+"或列表为空则返回原始号码
    """
    if not selected_numbers or len(selected_numbers) == 0:
        return ""
    
    phone_number = selected_numbers[0]
    
    if "+" in phone_number:
        # 找到"+"的位置并提取之后的所有字符
        plus_index = phone_number.find("+")
        return phone_number[plus_index + 1:]
    else:
        # 如果没有"+"，则返回原始号码
        return phone_number


# 主应用
st.set_page_config(page_title="Amazon Connect 通话记录查询工具", layout="wide")

st.title("Amazon Connect 通话记录查询工具")

# AWS凭证配置部分
with st.sidebar:
    st.header("AWS 配置")
    aws_region = st.text_input("AWS Region", "us-east-1")
    
    # 可选：用户可以直接输入AWS凭证，也可以使用AWS配置文件
    use_profile = st.checkbox("使用AWS配置文件", False)
    
    if use_profile:
        profile_name = st.text_input("配置文件名称", "default")
        session = boto3.Session(profile_name=profile_name)
    else:
        aws_access_key = st.text_input("AWS Access Key ID", "")
        aws_secret_key = st.text_input("AWS Secret Access Key", "", type="password")
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

# 初始化客户端
connect_client, s3_client = initialize_clients(session, aws_region)

# 主界面
instance_id = st.text_input("Amazon Connect Instance ID", 'b7e4b4ed-1bdf-4b14-b624-d9328f08725a')
association_id = st.text_input("Association ID (浏览器开发者工具搜索storage-configs?resourceType=CALL_RECORDINGS)", 'cc68693c3c2dd52d57e2afd87cd7e0c02439b45f199801d22128e1ba591d0b8a')
ctr_bucket = st.text_input('通话记录 S3 路径', value='s3://ctrvisualization1023/ctr-base/year=2025/month=05')
fetch_button = st.button("获取实例信息", key="fetch_instance_info")

# 状态变量，用于控制是否显示电话号码和录音路径
if 'show_instance_info' not in st.session_state:
    st.session_state.show_instance_info = False

# 当点击按钮时，设置状态为显示
if fetch_button and instance_id:
    st.session_state.show_instance_info = True
    st.session_state.instance_id = instance_id
    st.session_state.association_id = association_id
    st.session_state.ctr_bucket = ctr_bucket

# 如果状态为显示，则获取并显示电话号码和录音路径
if st.session_state.get('show_instance_info', False) and st.session_state.get('instance_id'):
    try:
        # 获取实例信息
        with st.spinner("正在获取实例信息..."):
            # 获取该实例的所有电话号码
            phone_numbers = get_all_phone_numbers(connect_client, st.session_state.instance_id)
            
            # 获取录音 S3 路径
            s3_path = get_call_recordings_s3_bucket(connect_client, st.session_state.instance_id, st.session_state.association_id)
            
            # 显示实例信息
            st.subheader("实例信息")
            
            # 显示录音 S3 路径
            st.write("通话录音 S3 路径:")
            st.code(s3_path)
            
            # 显示电话号码
            if phone_numbers:
                df_phone_numbers = pd.DataFrame(phone_numbers)
                st.write("该实例的热线电话号码:")
                st.dataframe(df_phone_numbers)
                
                # 选择电话号码
                selected_numbers = st.multiselect(
                    "选择要查询通话记录的电话号码",
                    df_phone_numbers['PhoneNumber'].tolist()
                )
                
                if selected_numbers:
                    # 添加按钮，点击后获取录音列表
                    if st.button("获取录音列表"):
                        with st.spinner("正在获取数据..."):
                            # 获取录音列表
                            recordings = get_call_recordings_list(s3_client, s3_path, selected_numbers)
                            
                            # 获取通话列表
                            contact_files = get_contact_files_list(s3_client, st.session_state.ctr_bucket)
                            
                            # 合并联系记录和录音记录
                            merged_records = merge_contacts_and_recordings(contact_files, recordings, selected_numbers)
                            
                            # 显示结果
                            st.subheader("查询结果")
                            
                            # 创建三个选项卡
                            tab1, tab2, tab3 = st.tabs(["录音列表", "通话列表", "合并列表"])
                            
                            # 录音列表选项卡
                            with tab1:
                                if recordings:
                                    st.write(f"共找到 {len(recordings)} 条录音记录")
                                    df_recordings = pd.DataFrame(recordings)
                                    
                                    # 固定每页显示100条记录
                                    items_per_page = 100
                                    
                                    # 分页显示
                                    total_pages = (len(df_recordings) + items_per_page - 1) // items_per_page
                                    page_number = st.number_input('页码', min_value=1, max_value=total_pages if total_pages > 0 else 1, value=1, key="recordings_page")
                                    
                                    start_idx = (page_number - 1) * items_per_page
                                    end_idx = min(start_idx + items_per_page, len(df_recordings))
                                    
                                    # 显示分页结果
                                    st.write(f"显示 {start_idx + 1} 到 {end_idx} 条记录，共 {len(df_recordings)} 条")
                                    
                                    st.dataframe(df_recordings.iloc[start_idx:end_idx])
                                else:
                                    st.info("未找到任何录音记录")
                            
                            # 通话列表选项卡
                            with tab2:
                                if contact_files:
                                    # 筛选出热线号码等于selected_numbers的记录
                                    df_contacts = pd.DataFrame(contact_files)
                                    if selected_numbers:
                                        df_contacts = df_contacts[df_contacts['热线号码'].isin(selected_numbers)]
                                    
                                    st.write(f"共找到 {len(df_contacts)} 条联系记录")
                                    
                                    # 固定每页显示100条记录
                                    items_per_page = 100
                                    
                                    # 分页显示
                                    total_pages = (len(df_contacts) + items_per_page - 1) // items_per_page
                                    page_number = st.number_input('页码', min_value=1, max_value=total_pages if total_pages > 0 else 1, value=1, key="contacts_page")
                                    
                                    start_idx = (page_number - 1) * items_per_page
                                    end_idx = min(start_idx + items_per_page, len(df_contacts))
                                    
                                    # 显示分页结果
                                    st.write(f"显示 {start_idx + 1} 到 {end_idx} 条记录，共 {len(df_contacts)} 条")
                                    
                                    # 显示联系记录
                                    st.dataframe(df_contacts.iloc[start_idx:end_idx])
                                else:
                                    st.info("未找到任何联系记录")
                            
                            # 合并列表选项卡
                            with tab3:
                                if merged_records:
                                    st.write(f"共找到 {len(merged_records)} 条合并记录")
                                    df_merged = pd.DataFrame(merged_records)
                                    
                                    # 固定每页显示100条记录
                                    items_per_page = 100
                                    
                                    # 分页显示
                                    total_pages = (len(df_merged) + items_per_page - 1) // items_per_page
                                    page_number = st.number_input('页码', min_value=1, max_value=total_pages if total_pages > 0 else 1, value=1, key="merged_page")
                                    
                                    start_idx = (page_number - 1) * items_per_page
                                    end_idx = min(start_idx + items_per_page, len(df_merged))
                                    
                                    # 显示分页结果
                                    st.write(f"显示 {start_idx + 1} 到 {end_idx} 条记录，共 {len(df_merged)} 条")
                                    
                                    # 显示合并记录
                                    st.dataframe(df_merged.iloc[start_idx:end_idx])
                                    
                                    # 添加下载全部录音按钮
                                    if st.button("下载全部录音"):
                                        with st.spinner("正在下载录音文件..."):
                                            result = download_recordings_to_directories(s3_client, merged_records, extract_number_after_plus(selected_numbers))
                                            if result:
                                                st.success(f"成功下载 {result['downloaded']} 个文件到 {result['base_dir']}，失败 {result['failed']} 个文件")
                                                st.info(f"文件已按热线号码组织到 {result['phone_groups']} 个目录中")
                                else:
                                    st.info("未找到任何合并记录")
            else:
                st.info("该实例未绑定任何电话号码")
                
    except Exception as e:
        st.error(f"获取实例信息时出错: {e}")