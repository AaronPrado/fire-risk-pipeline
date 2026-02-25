import boto3
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def sns_alert(df: pd.DataFrame, config: dict) -> None:
    sns = boto3.client("sns", region_name=config["aws"]["region"])

    df_alert = df[df["risk_level"].isin(["high", "very_high", "extreme"])]

    if df_alert.empty:
        logger.info("No high fire risk detected")
        return
    
    lines = []
    for _, row in df_alert.iterrows():
        lines.append(f"- {row['location']}: {row['risk_level']} ({row['risk_index']:.2f})")
    message = f"Fire Risk Alert - {df_alert['time'].iloc[0]}\n\nCities with high fire risk:\n" + "\n".join(lines)

    sns.publish(
        TopicArn=config["sns"]["topic_arn"],
        Message=message,
        Subject="High Fire Risk Alert",
    )

    logger.info(f"Alert sent to SNS topic {config['sns']['topic_arn']}")

    