package impact.productclassifier.feature

object StopWords {

  val STOP_WORDS: Array[String] = Array(
    "", " ", " ", "\t", "\t\t",
    "about", "also", "all", "an", "and", "any", "are", "as", "at", "allow", "add", "allows", "approx", "available", "around",
    "affordable", "added", "adds", "after", "additional", "above",
    "be", "by", "but", "best", "better", "back", "between", "been", "brand", "below", "buy", "before", "black",
    "can", "could", "check", "comes", "creative", "creativity", "collections", "colour", "color", "colors", "colours", "come",
    "choice", "choose", "christmas", "classic", "collection", "creating", "create",
    "different", "due", "difference", "do", "does", "design", "description", "designed", "double", "down", "delivery",
    "details", "dash", "day",
    "easy", "extra", "easily", "excellent", "etc", "even", "elegant", "either", "each", "ensure", "enough", "enjoy",
    "experience", "every",
    "for", "from", "free", "full", "features", "fit", "featuring", "find", "fully", "front", "feel",
    "good", "great", "get", "gift", "give", "go", "getting",
    "has", "have", "here", "high", "help", "how", "having", "helps",
    "if", "in", "is", "it", "its", "it's", "it’s", "include", "includes", "included", "into", "item", "including", "ideal",
    "just",
    "keep",
    "long", "look", "like", "light", "looking", "looks", "low", "luxury", "large", "life",
    "made", "make", "may", "more", "makes", "most", "making", "much", "multiple", "many",
    "no", "non", "not", "number", "new", "needs", "need",
    "of", "on", "or", "our", "other", "only", "off", "out", "over", "onto", "once", "own", "offer", "overall", "open", "original",
    "offers", "ones",
    "product", "perfect", "please", "piece", "plus", "provide", "provides", "premium", "put", "part", "products", "perfectly",
    "quality", "quick",
    "round", "range", "re",
    "stock", "so", "slightly", "set", "simple", "such", "suitable", "style", "stylish", "small", "saving", "save", "shop",
    "should", "sure", "same", "simply", "some", "store", "single", "see", "side", "super", "standard", "support",
    "take", "the", "to", "that", "this", "these", "they", "than", "their", "top", "there", "time", "thanks", "those",
    "technology", "then", "them", "through", "think", "today",
    "use", "up", "unique", "using", "under", "used", "us", "unlike",
    "we", "with", "without", "what", "where", "will", "when", "why", "who", "which", "while", "well", "way", "warranty",
    "white", "world", "want",
    "very",
    "you", "your", "yes",
    ":", "-", "–", "•", "&", "×", "+", "*", "/",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"
  )
  
  val EMPIRICAL: Array[String] = Array(
    "and", "all", "are", "an", "a", "at", "also", "any", "add", "after", "about", "actual", "allows", "allow", "around",
    "be", "by", "but", "best", "back", "before", "between",
    "can", "could", "come", "comes", "change", "choice", "choose",
    "different", "difference", "differences", "due", "do", "does",
    "each", "etc", "every", "even", "easy", "easily", "extra",
    "for", "from", "free", "four",
    "get", "got", "good", "give", "great",
    "has", "had", "have", "help",
    "in", "it", "its", "is", "item", "items", "including", "included", "includes", "into",
    "just",
    "keep",
    "like", "left",
    "make", "makes", "made", "more", "must", "making", "much", "most", "many", "might", "may",
    "nbsp", "not", "non", "needs", "need", "new",
    "of", "or", "on", "other", "our", "one", "over", "order", "off", "only", "out",
    "product", "products", "perfect", "put",
    "quality",
    "right",
    "so", "sure", "such", "some", "suitable",
    "the", "this", "that", "than", "then", "two", "they", "through", "them", "their", "there", "too",
    "very",
    "with", "which", "what", "was", "were", "will", "when", "without", "would", "way", "want", "while", "well",
    "you", "your", "yet",
  )
  
  val FOR_SORTED_BIGRAMS: Array[String] = Array(
    "and", "are", "all", "any", "an", "a", "as", "at", "also", "about",
    "be", "but", "by", "back",
    "can", "color", "com",
    "do", "design", "designed", "different",
    "easy", "each",
    "for", "from", "features", "free", "fit",
    "get", "got", "great",
    "has", "had", "have", "high", "how",
    "its", "in", "is", "it", "item", "included", "includes", "into",
    "keep",
    "like",
    "made", "more", "most", "make", "may", "might",
    "not", "new", "no", "non", "nbsp",
    "or", "of", "our", "out", "one", "other", "only", "on",
    "product", "products", "per", "package", "perfect", "please",
    "quality",
    "so", "some", "size", "suitable", "set",
    "the", "that", "to", "this", "these", "they", "their", "them", "there", "than", "time", "then",
    "use", "used",
    "very",
    "with", "which", "what", "was", "were", "will", "when", "weight",
    "you", "your", "yes"
  )

  val LDA: Array[String] = Array(
    "<int_>", 
    "and", "are", "all", "any", "an", "a", "as", "at", "also", "available", "about",
    "be", "but", "by", "back", "buy", "been",
    "can", "color", "com",
    "do", "design", "designed", "different", "description",
    "easy", "each", "etc",
    "for", "from", "features", "free",
    "get", "got", "great",
    "has", "had", "have", "high", "how",
    "its", "in", "is", "it", "item", "included", "includes", "into",
    "keep",
    "like",
    "made", "more", "most", "make", "may", "material", "might",
    "not", "new", "no", "non", "nbsp", "now",
    "or", "of", "our", "out", "one", "other", "only", "on", "original",
    "product", "products", "per", "package", "perfect", "please",
    "quality",
    "so", "some", "size", "suitable", "set",
    "the", "that", "to", "this", "these", "they", "their", "them", "there", "than", "time", "then", "thing",
    "use", "used",
    "very",
    "with", "which", "what", "was", "were", "will", "when", "weight", "want", "without", "while",
    "you", "your", "yes"
  )
}
