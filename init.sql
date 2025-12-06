-- Video Metadata Table
CREATE TABLE videos (
    id UUID PRIMARY KEY,
    filename TEXT NOT NULL,
    status TEXT DEFAULT 'processing',
    minio_path TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Detailed Analysis Per Sentence
CREATE TABLE analysis_segments (
    id SERIAL PRIMARY KEY,
    video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
    speaker_role TEXT, -- 'therapist' or 'patient'
    text_content TEXT,
    topic TEXT,        
    emotion TEXT,      
    confidence_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- High-Level Insights Per Video
CREATE TABLE analysis_summary (
    video_id UUID REFERENCES videos(id) ON DELETE CASCADE PRIMARY KEY,       
    summary_text TEXT,
    recommendations JSONB,
    cognitive_distortions JSONB,    
    therapist_interventions JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX idx_videos_status ON videos(status);
CREATE INDEX idx_analysis_segments_video_id ON analysis_segments(video_id);
CREATE INDEX idx_analysis_segments_speaker ON analysis_segments(speaker_role);
CREATE INDEX idx_analysis_segments_topic ON analysis_segments(topic);
