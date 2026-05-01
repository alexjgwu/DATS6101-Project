import matplotlib.pyplot as plt
import numpy as np

tasks = [f"Task {i}" for i in range(1, 11)]
sql_averages = [0.0006, 0.0101, 0.0258, 0.0719, 0.0918, 0.0041, 0.2147, 0.0232, 0.0179, 0.0520]
mongo_averages = [0.0011, 0.0117, 0.0298, 0.2938, 0.1975, 0.0073, 0.1489, 0.0438, 0.0581, 0.2166]

x = np.arange(len(tasks))
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))


rects1 = ax.bar(x - width/2, sql_averages, width, label='SQL (MySQL)', color='#3498db')
rects2 = ax.bar(x + width/2, mongo_averages, width, label='MongoDB', color='#2ecc71')


ax.set_ylabel('Average Execution Time (Seconds)')
ax.set_title('Performance Comparison: SQL vs MongoDB (10 Tasks)')
ax.set_xticks(x)
ax.set_xticklabels(tasks)
ax.legend()


def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.annotate(f'{height:.4f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)

autolabel(rects1)
autolabel(rects2)


fig.tight_layout()

plt.show()