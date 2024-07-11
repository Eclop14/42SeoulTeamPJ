import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm
import sys
import matplotlib.colors as mcolors

# 한글 폰트 설정
if sys.platform.startswith('win'):
    font_path = 'C:/Windows/Fonts/malgun.ttf'
elif sys.platform.startswith('darwin'):
    font_path = '/System/Library/Fonts/AppleSDGothicNeo.ttc'
else:
    font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
font_name = fm.FontProperties(fname=font_path).get_name()
plt.rc('font', family=font_name)
plt.rcParams['axes.unicode_minus'] = False

# 데이터 불러오기
df_2018 = pd.read_csv('C:/Users/ASUS/Desktop/Downloads/Merge/MergeY/merged_data_2018_processed.csv')
df_2020 = pd.read_csv('C:/Users/ASUS/Desktop/Downloads/Merge/MergeY/merged_data_2020_processed.csv')
df_2023 = pd.read_csv('C:/Users/ASUS/Desktop/Downloads/Merge/MergeY/merged_data_2023_processed.csv')

df_2018['YEAR'] = 2018
df_2020['YEAR'] = 2020
df_2023['YEAR'] = 2023

# 모든 데이터를 하나의 데이터프레임으로 병합
df = pd.concat([df_2018, df_2020, df_2023], ignore_index=True)

# 필요한 열 찾기
card_columns = [col for col in df.columns if 'CARD' in col and 'GRAMT' in col]
card_column = card_columns[0] if card_columns else None

if not card_column:
    print("카드 사용액 열을 찾을 수 없습니다.")
    sys.exit()

columns_of_interest = ['YEAR', 'MM_INCM', card_column, 'CR_NTSL_USE_GRAMT_AVG', 'CSADVC_USE_GRAMT_AVG']
df_analysis = df[columns_of_interest]

# 변수명 한글 매핑
column_mapping = {
    'MM_INCM': '월 소득',
    card_column: '카드 사용 금액 평균',
    'CR_NTSL_USE_GRAMT_AVG': '텔레세일즈 사용 금액 평균',
    'CSADVC_USE_GRAMT_AVG': 'CS 선수금 사용 금액 평균'
}

# 데이터프레임 열 이름 변경
df_analysis = df_analysis.rename(columns=column_mapping)

# 관심 변수 리스트 업데이트
columns_of_interest = ['YEAR'] + list(column_mapping.values())

# 1. 시계열 선 그래프
plt.figure(figsize=(12, 6))
for col in columns_of_interest[1:]:
    plt.plot(df_analysis.groupby('YEAR')[col].mean().index, 
             df_analysis.groupby('YEAR')[col].mean().values, 
             marker='o', label=col)
plt.title('연도별 평균 추이', fontsize=16, pad=20)
plt.xlabel('연도')
plt.ylabel('금액')
plt.legend()
plt.grid(True)
plt.show()

# 2. 막대 그래프
plt.figure(figsize=(16, 8))
df_mean = df_analysis.groupby('YEAR').mean()
df_mean.plot(kind='bar', width=0.8)
plt.title('연도별 평균 비교', fontsize=16, pad=20)
plt.xlabel('연도', fontsize=12)
plt.ylabel('금액', fontsize=12)
plt.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=10)
plt.xticks(rotation=0)
for i in range(len(df_mean.columns)):
    plt.bar_label(plt.gca().containers[i], label_type='edge', fontsize=8, padding=2)
plt.tight_layout()
plt.show()

# 3. 박스 플롯: 데이터의 분포를 더 명확하게 보기 위해 이상치를 제외한 박스 플롯을 그립니다.
plt.figure(figsize=(12, 6))
sns.boxplot(x='YEAR', y='value', hue='variable', 
            data=pd.melt(df_analysis, id_vars=['YEAR'], value_vars=columns_of_interest[1:]), showfliers=False)
plt.title('연도별 분포 비교 (이상치 제외)', fontsize=16, pad=20)
plt.ylabel('금액')
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.tight_layout()
plt.show()

# 4. 히트맵
plt.figure(figsize=(14, 12))
sns.heatmap(df_analysis.corr(), annot=True, cmap='coolwarm', fmt='.3f', annot_kws={"size": 10})
plt.title('변수 간 상관관계')
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
plt.show()

# 5. 산점도 (수정된 부분)
plt.figure(figsize=(18, 12))

# 월 소득을 1000 단위로 나누기 (더 세세한 구분)
df_analysis['월 소득 구간'] = (df_analysis['월 소득'] // 1000) * 1000

# 카드 사용액을 2000 단위로 나누고 300,000 이하만 유지 (하단 분포에 집중)
df_analysis['카드 사용액 구간'] = (df_analysis['카드 사용 금액 평균'] // 2000) * 2000
df_analysis = df_analysis[df_analysis['카드 사용액 구간'] <= 300000]

# 지터 효과를 위한 랜덤 오프셋 생성
df_analysis['x_jitter'] = df_analysis['월 소득 구간'] + np.random.uniform(-400, 400, df_analysis.shape[0])
df_analysis['y_jitter'] = df_analysis['카드 사용액 구간'] + np.random.uniform(-800, 800, df_analysis.shape[0])

# 산점도 그리기
sns.scatterplot(data=df_analysis, 
                x='x_jitter', y='y_jitter', 
                hue='YEAR', size='YEAR', sizes=(30, 100), alpha=0.5)

plt.title('월 소득 대비 카드 사용액', fontsize=20, pad=20)
plt.xlabel('월 소득', fontsize=16)
plt.ylabel('카드 사용액', fontsize=16)
plt.legend(title='연도', fontsize=12, title_fontsize=14)

# x축 눈금 설정
x_max = 340000  # 최대 340,000까지 표시
plt.xticks(range(0, x_max + 2000, 2000), rotation=45, ha='right')
plt.xlim(0, x_max)

# y축 눈금 설정
plt.ylim(0, 300000)
plt.yticks(range(0, 300000 + 2500, 2500))

# 축 레이블 포맷 지정
plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

# 그리드 추가
plt.grid(True, alpha=0.3, linestyle='--')

# 데이터 밀도를 표현하기 위한 2D 히스토그램 추가
plt.hist2d(df_analysis['x_jitter'], df_analysis['y_jitter'], bins=[170, 150], cmap='Blues', alpha=0.3, norm=mcolors.LogNorm())

plt.tight_layout()
plt.show()

# 6. 파이 차트
plt.figure(figsize=(30, 10))

plt.subplot(1, 3, 1)
df_analysis[df_analysis['YEAR'] == 2018][columns_of_interest[1:]].mean().plot(kind='pie', autopct='%1.1f%%', textprops={'fontsize': 12})
plt.title('2018년 지출 비중', fontsize=16, pad=20)

plt.subplot(1, 3, 2)
df_analysis[df_analysis['YEAR'] == 2020][columns_of_interest[1:]].mean().plot(kind='pie', autopct='%1.1f%%', textprops={'fontsize': 12})
plt.title('2020년 지출 비중', fontsize=16, pad=20)

plt.subplot(1, 3, 3)
df_analysis[df_analysis['YEAR'] == 2023][columns_of_interest[1:]].mean().plot(kind='pie', autopct='%1.1f%%', textprops={'fontsize': 12})
plt.title('2023년 지출 비중', fontsize=16, pad=20)

plt.tight_layout()
plt.show()