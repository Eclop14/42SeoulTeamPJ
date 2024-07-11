import pandas as pd
import os

# CSV 파일들이 있는 디렉토리 경로
directory = 'C:/Users/User1/Downloads/Mergecsv'

# 원하는 연도 리스트
target_years = ['2018', '2020', '2023']

# 연도별 데이터프레임을 저장할 딕셔너리
year_dataframes = {year: [] for year in target_years}

# 디렉토리 내의 모든 CSV 파일을 읽어 2018년, 2020년, 2023년 데이터 추출
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        filepath = os.path.join(directory, filename)
        df = pd.read_csv(filepath)
        
        if 'CRTR_QURTR' in df.columns:
            df['CRTR_QURTR'] = df['CRTR_QURTR'].astype(str)
            for year in target_years:
                year_df = df[df['CRTR_QURTR'].str[:4] == year]
                if not year_df.empty:
                    year_dataframes[year].append(year_df)
                    print(f"파일 '{filename}'에서 {year}년 데이터 {len(year_df)} 개의 행이 추가되었습니다.")

# 연도별로 데이터프레임 병합 및 저장
output_dir = 'C:/Users/User1/Downloads/Merge/MergeY'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for year in target_years:
    if year_dataframes[year]:
        merged_df = pd.concat(year_dataframes[year], ignore_index=True)
        
        print(f"\n{year}년 병합된 데이터프레임 정보:")
        print(merged_df.info())
        
        # 결측치 확인
        print(f"\n{year}년 결측치 확인:")
        missing_values = merged_df.isnull().sum()
        missing_percentages = 100 * merged_df.isnull().sum() / len(merged_df)
        missing_table = pd.concat([missing_values, missing_percentages], axis=1, keys=['Missing Values', 'Percentage'])
        print(missing_table[missing_table['Missing Values'] > 0])
        
        # 처리된 데이터프레임을 CSV 파일로 저장
        output_filename = f'merged_data_{year}_processed.csv'
        output_path = os.path.join(output_dir, output_filename)
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n{year}년 처리된 데이터가 저장되었습니다: {output_path}")
    else:
        print(f"\n{year}년 처리할 수 있는 데이터가 없습니다.")

print("\n처리된 총 연도 수:", sum(1 for dfs in year_dataframes.values() if dfs))