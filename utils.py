import pickle
import torch
from collections import Counter
import string
import sys
from sklearn.metrics import accuracy_score

import pickle
import os

def build_vocab(file_path=None, max_len=10000, vocab_path=None):
    if vocab_path is not None and os.path.exists(vocab_path):
        with open(vocab_path, 'rb') as f:
            vocab = pickle.load(f)
        print(f"Vocab loaded from {vocab_path}")
        return vocab

    elif file_path:
        print(f"Vocab not found at {vocab_path}, building vocab from scratch.")

        with open(file_path, 'r', encoding='utf-8') as f:
            sentences = f.readlines()

        all_words = []
        for index, sentence in enumerate(sentences):
            sentence = sentence.strip().lower()
            sentence = sentence.translate(str.maketrans("", "", string.punctuation))
            all_words.extend(sentence.split())

            sys.stdout.write(f"\rProcessing {file_path}, {index + 1:6d} | {len(sentences)}")
            sys.stdout.flush()

        word_counts = Counter(all_words)

        # special token
        vocab = {
            "<pad>": 0,
            "<unk>": 1,
            "<seqstart>": 2,
            "<seqend>": 3
        }

        # create new id
        start_idx = len(vocab)
        for idx, (word, _) in enumerate(word_counts.most_common(max_len), start=start_idx):
            if word not in vocab:
                vocab[word] = idx

        print()

        if vocab_path is not None:
            with open(vocab_path, 'wb') as f:
                pickle.dump(vocab, f)

        return vocab

    else:
        raise ValueError("No vocab file found and no file_path provided to build from.")

def train(model, train_loader, valid_loader, criterion, optimizer, device, pad_token_id, num_epochs=10):
    train_losses = []
    valid_accuracies = []
    valid_losses = []

    for epoch in range(num_epochs):
        model.train()
        running_loss = 0.0
        total_batches = len(train_loader)
        
        for batch_idx, batch in enumerate(train_loader):
            encoder_input, decoder_target = batch
            encoder_input = encoder_input.to(device)
            decoder_target = decoder_target.to(device).long()
            
            tgt_input = decoder_target[:, :-1]
            tgt_out   = decoder_target[:, 1:]
            
            src_mask, tgt_mask, src_padding_mask, tgt_padding_mask = create_mask(encoder_input, tgt_input, device, pad_token_id)
            
            optimizer.zero_grad()

            outputs = model(encoder_input, tgt_input, src_mask, tgt_mask, src_padding_mask, tgt_padding_mask)
            
            loss = criterion(outputs.reshape(-1, outputs.shape[-1]), tgt_out.reshape(-1))
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            sys.stdout.write(f"\rEpoch [{epoch + 1:2d}/{num_epochs}] Batch [{batch_idx + 1}/{total_batches}], Loss: {loss.item():.4f}")
            sys.stdout.flush()

        avg_train_loss = running_loss / total_batches
        train_losses.append(avg_train_loss)

        valid_accuracy, valid_loss = evaluate(model, valid_loader, criterion, device, pad_token_id)
        valid_accuracies.append(valid_accuracy)
        valid_losses.append(valid_loss)

        print(f"\nEpoch [{epoch + 1:2d}/{num_epochs}], Avg Loss: {avg_train_loss:.4f}, Val Loss: {valid_loss:.4f}, Val Acc: {valid_accuracy:.4f}")

    return train_losses, valid_accuracies, valid_losses

def evaluate(model, valid_loader, criterion, device, pad_token_id):
    model.eval()
    all_preds = []
    all_labels = []
    running_loss = 0.0
    
    with torch.no_grad():
        for batch in valid_loader:
            encoder_input, decoder_target = batch
            encoder_input = encoder_input.to(device)
            decoder_target = decoder_target.to(device)
            

            tgt_input = decoder_target[:, :-1]
            tgt_out   = decoder_target[:, 1:]

            src_mask, tgt_mask, src_padding_mask, tgt_padding_mask = create_mask(encoder_input, tgt_input, device, pad_token_id)
            
            outputs = model(encoder_input, tgt_input, src_mask, tgt_mask, src_padding_mask, tgt_padding_mask)
            
            loss = criterion(outputs.reshape(-1, outputs.shape[-1]), tgt_out.reshape(-1))
            running_loss += loss.item()
            
            _, predicted = torch.max(outputs, dim=-1)
            all_preds.extend(predicted.reshape(-1).cpu().numpy())
            all_labels.extend(tgt_out.reshape(-1).cpu().numpy())
    
    accuracy = accuracy_score(all_labels, all_preds)
    avg_valid_loss = running_loss / len(valid_loader)
    return accuracy, avg_valid_loss

def generate_square_subsequent_mask(sz, DEVICE):
    mask = (torch.triu(torch.ones((sz, sz), device=DEVICE)) == 1).transpose(0, 1)
    mask = mask.float().masked_fill(mask == 0, float('-inf')).masked_fill(mask == 1, float(0.0))
    return mask

def create_mask(src, tgt, DEVICE, PAD_IDX):
    src_seq_len = src.shape[1]
    tgt_seq_len = tgt.shape[1]

    tgt_mask = generate_square_subsequent_mask(tgt_seq_len, DEVICE)
    src_mask = torch.zeros((src_seq_len, src_seq_len), device=DEVICE).type(torch.bool)

    src_padding_mask = (src == PAD_IDX)
    tgt_padding_mask = (tgt == PAD_IDX)
    return src_mask, tgt_mask, src_padding_mask, tgt_padding_mask
