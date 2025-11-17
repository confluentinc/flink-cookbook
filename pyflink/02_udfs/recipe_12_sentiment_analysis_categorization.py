# Recipe 12: Real-time Sentiment Analysis and Categorization with UDFs
from typing import List, Tuple, Dict, Any
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table.expressions import col
import re
import json

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, str, str]] = [
    (1, "I love this product! It's amazing and works perfectly.", "product_review", "customer_feedback"),
    (2, "This is terrible. Worst purchase ever. Don't buy it!", "product_review", "customer_feedback"),
    (3, "The service was okay, nothing special but not bad either.", "service_review", "customer_feedback"),
    (4, "Great customer support team, very helpful and friendly.", "service_review", "customer_feedback"),
    (5, "The delivery was late and the package was damaged.", "delivery_review", "customer_feedback"),
    (6, "Excellent quality and fast shipping. Highly recommended!", "product_review", "customer_feedback"),
    (7, "The app crashes constantly and is very frustrating to use.", "app_review", "customer_feedback"),
    (8, "Beautiful design and intuitive interface. Love it!", "app_review", "customer_feedback"),
    (9, "The food was cold and the waiter was rude.", "restaurant_review", "customer_feedback"),
    (10, "Amazing food and excellent service. Will definitely return!", "restaurant_review", "customer_feedback"),
    (11, None, None, None)
]

# Sentiment Analysis Dictionaries
POSITIVE_WORDS = {
    'love', 'great', 'excellent', 'amazing', 'perfect', 'wonderful', 'fantastic', 'outstanding',
    'superb', 'brilliant', 'awesome', 'terrific', 'fabulous', 'incredible', 'marvelous',
    'good', 'nice', 'fine', 'okay', 'decent', 'satisfactory', 'helpful', 'friendly',
    'fast', 'quick', 'efficient', 'reliable', 'quality', 'beautiful', 'intuitive'
}

NEGATIVE_WORDS = {
    'hate', 'terrible', 'awful', 'horrible', 'worst', 'bad', 'poor', 'disappointing',
    'frustrating', 'annoying', 'terrible', 'useless', 'broken', 'damaged', 'late',
    'slow', 'rude', 'cold', 'crash', 'problem', 'issue', 'difficult', 'confusing'
}

CATEGORY_KEYWORDS = {
    'product': {'product', 'item', 'purchase', 'buy', 'quality', 'works', 'broken'},
    'service': {'service', 'support', 'help', 'assistance', 'team', 'staff'},
    'delivery': {'delivery', 'shipping', 'package', 'arrived', 'late', 'damaged'},
    'app': {'app', 'application', 'interface', 'crashes', 'design', 'intuitive'},
    'restaurant': {'food', 'waiter', 'restaurant', 'meal', 'dining', 'service'}
}

# 1. Basic Sentiment Analysis UDF
@udf(result_type=DataTypes.STRING())
def analyze_sentiment(text: str) -> str:
    """Analyze sentiment of text (positive, negative, neutral)"""
    if text is None:
        return "neutral"
    
    try:
        text_lower = text.lower()
        words = re.findall(r'\b\w+\b', text_lower)
        
        positive_count = sum(1 for word in words if word in POSITIVE_WORDS)
        negative_count = sum(1 for word in words if word in NEGATIVE_WORDS)
        
        if positive_count > negative_count:
            return "positive"
        elif negative_count > positive_count:
            return "negative"
        else:
            return "neutral"
    except (AttributeError, TypeError):
        return "neutral"

# 2. Sentiment Score UDF
@udf(result_type=DataTypes.DOUBLE())
def calculate_sentiment_score(text: str) -> float:
    """Calculate sentiment score (-1.0 to 1.0)"""
    if text is None:
        return 0.0
    
    try:
        text_lower = text.lower()
        words = re.findall(r'\b\w+\b', text_lower)
        
        positive_count = sum(1 for word in words if word in POSITIVE_WORDS)
        negative_count = sum(1 for word in words if word in NEGATIVE_WORDS)
        
        total_words = len(words)
        if total_words == 0:
            return 0.0
        
        score = (positive_count - negative_count) / total_words
        return round(max(-1.0, min(1.0, score)), 2)
    except (AttributeError, TypeError, ZeroDivisionError):
        return 0.0

# 3. Content Categorization UDF
@udf(result_type=DataTypes.STRING())
def categorize_content(text: str) -> str:
    """Categorize content based on keywords"""
    if text is None:
        return "unknown"
    
    try:
        text_lower = text.lower()
        words = set(re.findall(r'\b\w+\b', text_lower))
        
        category_scores = {}
        for category, keywords in CATEGORY_KEYWORDS.items():
            score = len(words.intersection(keywords))
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            # Return category with highest score
            return max(category_scores, key=category_scores.get)
        else:
            return "general"
    except (AttributeError, TypeError):
        return "unknown"

# 4. Emotion Detection UDF
@udf(result_type=DataTypes.STRING())
def detect_emotion(text: str) -> str:
    """Detect primary emotion in text"""
    if text is None:
        return "neutral"
    
    try:
        text_lower = text.lower()
        
        # Simple emotion detection based on keywords
        if any(word in text_lower for word in ['love', 'amazing', 'excellent', 'wonderful']):
            return "joy"
        elif any(word in text_lower for word in ['hate', 'terrible', 'awful', 'horrible']):
            return "anger"
        elif any(word in text_lower for word in ['sad', 'disappointed', 'upset']):
            return "sadness"
        elif any(word in text_lower for word in ['frustrated', 'annoyed', 'irritated']):
            return "frustration"
        elif any(word in text_lower for word in ['surprised', 'shocked', 'unexpected']):
            return "surprise"
        else:
            return "neutral"
    except (AttributeError, TypeError):
        return "neutral"

# 5. Comprehensive Analysis UDF
class ComprehensiveAnalysisUDF(ScalarFunction):
    """Comprehensive sentiment and categorization analysis"""
    
    def eval(self, text: str) -> str:
        if text is None:
            return json.dumps({
                "sentiment": "neutral",
                "sentiment_score": 0.0,
                "category": "unknown",
                "emotion": "neutral",
                "word_count": 0
            })
        
        try:
            text_lower = text.lower()
            words = re.findall(r'\b\w+\b', text_lower)
            word_count = len(words)
            
            # Calculate sentiment
            positive_count = sum(1 for word in words if word in POSITIVE_WORDS)
            negative_count = sum(1 for word in words if word in NEGATIVE_WORDS)
            
            if positive_count > negative_count:
                sentiment = "positive"
            elif negative_count > positive_count:
                sentiment = "negative"
            else:
                sentiment = "neutral"
            
            # Calculate sentiment score
            sentiment_score = 0.0
            if word_count > 0:
                sentiment_score = round((positive_count - negative_count) / word_count, 2)
                sentiment_score = max(-1.0, min(1.0, sentiment_score))
            
            # Categorize content
            words_set = set(words)
            category_scores = {}
            for category, keywords in CATEGORY_KEYWORDS.items():
                score = len(words_set.intersection(keywords))
                if score > 0:
                    category_scores[category] = score
            
            category = max(category_scores, key=category_scores.get) if category_scores else "general"
            
            # Detect emotion
            emotion = detect_emotion(text)
            
            return json.dumps({
                "sentiment": sentiment,
                "sentiment_score": sentiment_score,
                "category": category,
                "emotion": emotion,
                "word_count": word_count,
                "positive_words": positive_count,
                "negative_words": negative_count
            })
        except (AttributeError, TypeError, ZeroDivisionError):
            return json.dumps({
                "sentiment": "neutral",
                "sentiment_score": 0.0,
                "category": "unknown",
                "emotion": "neutral",
                "word_count": 0
            })

comprehensive_analysis_udf = udf(ComprehensiveAnalysisUDF(), result_type=DataTypes.STRING())

# 6. Urgency Detection UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_urgent(text: str) -> bool:
    """Detect if text indicates urgency"""
    if text is None:
        return False
    
    try:
        urgency_words = {'urgent', 'emergency', 'critical', 'immediate', 'asap', 'now', 'quickly'}
        text_lower = text.lower()
        words = set(re.findall(r'\b\w+\b', text_lower))
        
        return bool(words.intersection(urgency_words))
    except (AttributeError, TypeError):
        return False

def recipe_12_sentiment_analysis_categorization() -> None:
    """
    Demonstrates real-time sentiment analysis and categorization using UDFs in PyFlink.
    
    This function shows:
    1. Basic sentiment analysis (positive, negative, neutral)
    2. Sentiment scoring (-1.0 to 1.0)
    3. Content categorization based on keywords
    4. Emotion detection
    5. Comprehensive analysis with multiple metrics
    6. Urgency detection
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 12: Real-time Sentiment Analysis and Categorization with UDFs")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("text", DataTypes.STRING()),
            DataTypes.FIELD("expected_category", DataTypes.STRING()),
            DataTypes.FIELD("source", DataTypes.STRING())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Basic Sentiment Analysis
    print("\n--- 1. Basic Sentiment Analysis ---")
    sentiment_result = input_table.select(
        col("id"),
        col("text"),
        analyze_sentiment(col("text")).alias("sentiment")
    )
    print("Table after sentiment analysis:")
    sentiment_result.print_schema()
    print("\nData after sentiment analysis:")
    sentiment_result.execute().print()

    # 2. Sentiment Scoring
    print("\n--- 2. Sentiment Scoring ---")
    score_result = input_table.select(
        col("id"),
        col("text"),
        calculate_sentiment_score(col("text")).alias("sentiment_score")
    )
    print("Table after sentiment scoring:")
    score_result.print_schema()
    print("\nData after sentiment scoring:")
    score_result.execute().print()

    # 3. Content Categorization
    print("\n--- 3. Content Categorization ---")
    category_result = input_table.select(
        col("id"),
        col("text"),
        col("expected_category"),
        categorize_content(col("text")).alias("detected_category")
    )
    print("Table after content categorization:")
    category_result.print_schema()
    print("\nData after content categorization:")
    category_result.execute().print()

    # 4. Emotion Detection
    print("\n--- 4. Emotion Detection ---")
    emotion_result = input_table.select(
        col("id"),
        col("text"),
        detect_emotion(col("text")).alias("emotion")
    )
    print("Table after emotion detection:")
    emotion_result.print_schema()
    print("\nData after emotion detection:")
    emotion_result.execute().print()

    # 5. Comprehensive Analysis
    print("\n--- 5. Comprehensive Analysis ---")
    comprehensive_result = input_table.select(
        col("id"),
        col("text"),
        comprehensive_analysis_udf(col("text")).alias("analysis_result")
    )
    print("Table after comprehensive analysis:")
    comprehensive_result.print_schema()
    print("\nData after comprehensive analysis:")
    comprehensive_result.execute().print()

    # 6. Urgency Detection
    print("\n--- 6. Urgency Detection ---")
    urgency_result = input_table.select(
        col("id"),
        col("text"),
        is_urgent(col("text")).alias("is_urgent")
    )
    print("Table after urgency detection:")
    urgency_result.print_schema()
    print("\nData after urgency detection:")
    urgency_result.execute().print()

    # 7. Combined Analysis with SQL
    print("\n--- 7. Combined Analysis with SQL ---")
    # Register UDFs for SQL
    t_env.create_temporary_function("ANALYZE_SENTIMENT", analyze_sentiment)
    t_env.create_temporary_function("CALC_SENTIMENT_SCORE", calculate_sentiment_score)
    t_env.create_temporary_function("CATEGORIZE_CONTENT", categorize_content)
    t_env.create_temporary_function("DETECT_EMOTION", detect_emotion)
    t_env.create_temporary_function("IS_URGENT", is_urgent)

    t_env.create_temporary_view("feedback_data", input_table)
    combined_result = t_env.sql_query("""
        SELECT 
            id,
            text,
            ANALYZE_SENTIMENT(text) as sentiment,
            CALC_SENTIMENT_SCORE(text) as sentiment_score,
            CATEGORIZE_CONTENT(text) as category,
            DETECT_EMOTION(text) as emotion,
            IS_URGENT(text) as is_urgent,
            CASE 
                WHEN CALC_SENTIMENT_SCORE(text) > 0.3 THEN 'high_priority'
                WHEN CALC_SENTIMENT_SCORE(text) < -0.3 THEN 'urgent_review'
                ELSE 'normal_priority'
            END as priority_level
        FROM feedback_data
        WHERE text IS NOT NULL
    """)
    print("Table from combined SQL analysis:")
    combined_result.print_schema()
    print("\nData from combined SQL analysis:")
    combined_result.execute().print()

if __name__ == '__main__':
    recipe_12_sentiment_analysis_categorization() 