# Module 20: Main Pipeline Execution

if __name__ == "__main__":
    # Initialize logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize and run pipeline
        orchestrator = PipelineOrchestrator()
        
        # Start API service in background
        import threading
        api_thread = threading.Thread(
            target=lambda: uvicorn.run(app, host="0.0.0.0", port=8000),
            daemon=True
        )
        api_thread.start()
        
        # Run daily batch pipeline
        orchestrator.run_daily_pipeline()
        
        # Start real-time processing
        query = orchestrator.run_real_time_pipeline()
        query.awaitTermination()
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        ErrorHandler().handle_error(e, "Main pipeline execution")
        raise